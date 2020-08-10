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
import java.io.InputStream;
import java.util.*;
import java.util.function.Consumer;

import com.google.privacy.dlp.v2.Table;
import com.google.privacy.dlp.v2.Value;
import com.google.protobuf.ByteString;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reads all records in the given split (i.e. a group of Avro data blocks) and then converts
 * those records to DLP table rows.
 */
public class ReadAvroBlocks extends DoFn<KV<String, ByteString>, KV<String, Table.Row>> {

    public static final Logger LOG = LoggerFactory.getLogger(ReadAvroBlocks.class);

    private final Integer keyRange;
    public final String columnDelimiter;

    public ReadAvroBlocks(Integer keyRange, String columnDelimiter) {
        this.keyRange = keyRange;
        this.columnDelimiter = columnDelimiter;
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
                        stack.push(value);
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

    @ProcessElement
    public void processElement(ProcessContext c) throws IOException {
        String[] inputKey = c.element().getKey().split("~");
        String fileName = inputKey[0];

        ByteString contents = c.element().getValue();
        InputStream inputStream = contents.newInput();
        DatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
        DataFileStream<GenericRecord> streamReader = new DataFileStream<>(inputStream, datumReader);
        GenericRecord record = new GenericData.Record(streamReader.getSchema());

        // Loop through every record in the split
        while(streamReader.hasNext()) {
            streamReader.next(record);

            // Convert Avro record to DLP table row
            Table.Row.Builder rowBuilder = Table.Row.newBuilder();
            getFlattenedValues(record, (Object value) -> {
                if (value == null) {
                    rowBuilder.addValues(Value.newBuilder().setStringValue("").build());
                }
                else {
                    rowBuilder.addValues(Value.newBuilder().setStringValue(value.toString()).build());
                }
            });

            // Output the DLP table row
            String outputKey = String.format("%s~%d", fileName, new Random().nextInt(keyRange));
            c.outputWithTimestamp(KV.of(outputKey, rowBuilder.build()), Instant.now());
        }
        streamReader.close();
        inputStream.close();
    }

}
