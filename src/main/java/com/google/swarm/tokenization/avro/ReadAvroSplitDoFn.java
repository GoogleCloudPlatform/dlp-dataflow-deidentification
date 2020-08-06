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
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.beam.sdk.io.FileIO.ReadableFile;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reads all records in the given split (i.e. a group of avro data blocks) and then converts
 * those records to DLP table rows.
 */
public class ReadAvroSplitDoFn extends DoFn<KV<String, ReadableFile>, KV<String, String>> {

    public static final Logger LOG = LoggerFactory.getLogger(ReadAvroSplitDoFn.class);

    private final Integer keyRange;
    public final CSVFormat csvFormat;

    public ReadAvroSplitDoFn(Integer keyRange, String delimiter) {
        this.keyRange = keyRange;
        this.csvFormat = CSVFormat.newFormat(delimiter.charAt(0));
    }

    @ProcessElement
    public void processElement(ProcessContext c) throws IOException {
        String[] inputKey = c.element().getKey().split("~");
        String fileName = inputKey[0];
        long fromPosition = Long.parseLong(inputKey[1]);
        long toPosition = Long.parseLong(inputKey[2]);
        ReadableFile file = c.element().getValue();

        try (AvroUtil.AvroSeekableByteChannel channel = AvroUtil.getChannel(file)) {
            DatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
            DataFileReader<GenericRecord> fileReader = new DataFileReader<>(channel, datumReader);
            GenericRecord record = new GenericData.Record(fileReader.getSchema());

            // Move to the beginning of the split
            fileReader.sync(fromPosition);

            // Loop through all records within the split
            while(fileReader.hasNext() && !fileReader.pastSync(toPosition)) {
                // Read next record
                fileReader.next(record);

                // Loop through all of the record's fields
                List<Schema.Field> fields = fileReader.getSchema().getFields();

                // Convert Avro record to CSV record
                StringBuffer sb = new StringBuffer();
                List<String> valueList = new ArrayList<>();
                for (Schema.Field field : fields) {
                    Object value = record.get(field.name());
                    if (value == null) {
                        valueList.add("");
                    }
                    else {
                        valueList.add(value.toString());
                    }
                }
                CSVPrinter printer = new CSVPrinter(sb, csvFormat);
                printer.printRecord(valueList);

                // Output the DLP table row
                String outputKey = String.format("%s~%d", fileName, new Random().nextInt(keyRange));
                c.outputWithTimestamp(KV.of(outputKey, sb.toString()), Instant.now());
            }
        }
    }

}
