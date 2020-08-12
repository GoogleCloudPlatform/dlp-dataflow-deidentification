/*
 * Copyright 2020 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.swarm.tokenization.avro;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.function.Consumer;

import static org.apache.avro.file.DataFileConstants.SYNC_SIZE;
import com.google.protobuf.ByteString;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.SeekableInput;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.beam.sdk.io.FileIO;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.joda.time.format.DateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Various helpers for working with Avro files.
 */
public class AvroUtil {

    public static final Logger LOG = LoggerFactory.getLogger(AvroUtil.class);

    /**
     * Returns the sync marker used by the given Avro file.
     * Each Avro file has its own randomly-generated sync marker that separates every data block.
     */
    public static ByteString extractSyncMarker(SeekableInput file) throws IOException {
        DatumReader<GenericRecord> reader = new GenericDatumReader<>();
        DataFileReader<GenericRecord> fileReader = new DataFileReader<>(file, reader);

        // Move to the first data block
        fileReader.sync(0);

        // Create a buffer for the syn marker
        byte[] buffer = new byte[SYNC_SIZE];

        // Move back by SYNC_SIZE (16) bytes
        file.seek(file.tell() - SYNC_SIZE);

        // Read the sync marker into the buffer
        file.read(buffer, 0, SYNC_SIZE);
        fileReader.close();
        return ByteString.copyFrom(buffer);
    }

    /**
     * Returns in the header (in binary form).
     */
    public static ByteString extractHeader(FileIO.ReadableFile file) throws IOException {
        try (AvroUtil.AvroSeekableByteChannel channel = AvroUtil.getChannel(file)) {
            DatumReader<GenericRecord> reader = new GenericDatumReader<>();
            DataFileReader<GenericRecord> fileReader = new DataFileReader<>(channel, reader);

            // Move to the first data block to get the size of the header
            fileReader.sync(0);
            int headerSize = (int) channel.tell();

            // Create a buffer for the header
            byte[] buffer = new byte[headerSize];

            // Move back to the beginning of the file and read the header into the buffer
            channel.seek(0);
            channel.read(buffer, 0, headerSize);
            fileReader.close();
            return ByteString.copyFrom(buffer);
        }
    }

    /**
     * Returns the list of field names from the given schema. Calls itself recursively
     * to flatten nested fields.
     */
    public static void flattenFieldNames(Schema schema, List<String> fieldNames, String prefix) {
        for (Schema.Field field : schema.getFields()) {
            if (field.schema().getType() == Schema.Type.RECORD) {
                flattenFieldNames(field.schema(), fieldNames, prefix + field.name() + ".");
            }
            else {
                fieldNames.add(prefix + field.name());
            }
        }
    }

    /**
     * Converts the given Avro value to a type that can be processed by DLP
     */
    public static Object convertForDLP(Object value, LogicalType logicalType) {
        if (logicalType != null) {
            if (logicalType instanceof LogicalTypes.Date) {
                Instant instant = Instant.EPOCH.plus(Duration.standardDays((int) value));
                return DateTimeFormat.forPattern("yyyy-MM-dd").print(instant);
            }
            else if (logicalType instanceof LogicalTypes.TimestampMillis) {
                Instant instant = new Instant((int) value);
                return DateTimeFormat.forPattern("yyyy-MM-dd hh:mm:ss").print(instant);
            }
            else if (logicalType instanceof LogicalTypes.TimestampMicros) {
                Instant instant = new Instant((int) value / 1000);
                return DateTimeFormat.forPattern("yyyy-MM-dd hh:mm:ss").print(instant);
            }
        }
        return value;
    }

    /**
     * Substitute for a `null` value used in the Avro value flattening function below.
     */
    private static class NullValue {}

    /**
     * Traverses the given Avro record's (potentially nested) schema and
     * sends all flattened values to the given consumer.
     * Uses a stack to perform an iterative, preorder traversal of the schema tree.
     */
    public static void getFlattenedValues(GenericRecord rootNode, Consumer<Object> consumer) {
        Deque<Object> stack = new ArrayDeque<>();
        // Start with the given record
        stack.push(rootNode);
        while(!stack.isEmpty()) {
            Object node = stack.pop();
            if (node instanceof GenericRecord) {
                // The current node is a record, so we go one level deeper...
                GenericRecord record = (GenericRecord) node;
                List<Schema.Field> fields = record.getSchema().getFields();
                ListIterator<Schema.Field> iterator = fields.listIterator(fields.size());
                // Push all of the record's sub-fields to the stack in reverse order
                // to prioritize sub-fields from left to right in subsequent loop iterations.
                while (iterator.hasPrevious()) {
                    String fieldName = iterator.previous().name();
                    Object value = record.get(fieldName);
                    if (value == null) {
                        // Special case: A stack can't accept a null value,
                        // so we substitute for a mock object instead.
                        stack.push(new NullValue());
                    }
                    else {
                        LogicalType logicalType = record.getSchema().getField(fieldName).schema().getLogicalType();
                        Object convertedValue = AvroUtil.convertForDLP(value, logicalType);
                        stack.push(convertedValue);
                    }
                }
            }
            else {
                // The current node is a simple value.
                if (node instanceof NullValue) {
                    // Substitute the mock NullValue object for an actual `null` value.
                    consumer.accept(null);
                }
                else {
                    consumer.accept(node);
                }
            }
        }
    }

    /**
     * Byte channel that enables random access for Avro files.
     */
    public static class AvroSeekableByteChannel implements SeekableInput {

        private final SeekableByteChannel channel;

        public AvroSeekableByteChannel(SeekableByteChannel channel) {
            this.channel = channel;
        }

        @Override
        public void seek(long p) throws IOException {
            channel.position(p);
        }

        @Override
        public long tell() throws IOException {
            return channel.position();
        }

        @Override
        public long length() throws IOException {
            return channel.size();
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            ByteBuffer buffer = ByteBuffer.wrap(b, off, len);
            return channel.read(buffer);
        }

        @Override
        public void close() throws IOException {
            channel.close();
        }
    }

    /**
     * Converts a ReadableFile to a byte channel enabled for random access.
     */
    public static AvroSeekableByteChannel getChannel(FileIO.ReadableFile file) {
        SeekableByteChannel channel;
        try {
            channel = file.openSeekable();
        } catch (IOException e) {
            LOG.error("Failed to open file {}", e.getMessage());
            throw new RuntimeException(e);
        }
        return new AvroSeekableByteChannel(channel);
    }

}
