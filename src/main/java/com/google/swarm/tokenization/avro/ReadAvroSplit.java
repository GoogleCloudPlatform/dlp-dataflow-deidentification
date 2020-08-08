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
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

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
 * those records to DLP rows.
 */
public class ReadAvroSplit extends DoFn<KV<String, ByteString>, KV<String, Table.Row>> {

    public static final Logger LOG = LoggerFactory.getLogger(ReadAvroSplit.class);

    private final Integer keyRange;
    public final String columnDelimiter;

    public ReadAvroSplit(Integer keyRange, String columnDelimiter) {
        this.keyRange = keyRange;
        this.columnDelimiter = columnDelimiter;
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
        List<Schema.Field> fields = streamReader.getSchema().getFields();

        // Loop through every record in the split
        while(streamReader.hasNext()) {
            streamReader.next(record);

            // Convert Avro record to DLP Row
            Table.Row.Builder rowBuilder = Table.Row.newBuilder();
            for (Schema.Field field : fields) {
                Object value = record.get(field.name());
                // Insert current record field's value into the DLP table row
                if (value != null) {
                    rowBuilder.addValues(Value.newBuilder().setStringValue(value.toString()).build());
                }
                else {
                    rowBuilder.addValues(Value.newBuilder().setStringValue("").build());
                }
            }

            // Output the CSV record
            String outputKey = String.format("%s~%d", fileName, new Random().nextInt(keyRange));
            c.outputWithTimestamp(KV.of(outputKey, rowBuilder.build()), Instant.now());
        }
        streamReader.close();
        inputStream.close();
    }

}
