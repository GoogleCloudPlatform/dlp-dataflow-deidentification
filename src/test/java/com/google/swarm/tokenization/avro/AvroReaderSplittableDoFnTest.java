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

import com.google.privacy.dlp.v2.Table;
import com.google.privacy.dlp.v2.Value;
import java.io.*;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.io.DatumWriter;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.PCollection;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class AvroReaderSplittableDoFnTest {

  @Rule public transient TestPipeline pipeline = TestPipeline.create();

  @ClassRule public static transient TemporaryFolder tmpFolder = new TemporaryFolder();

  public static final Schema schema =
      SchemaBuilder.record("root")
          .fields()
          .name("user")
          .type()
          .stringType()
          .noDefault()
          .name("message")
          .type()
          .stringType()
          .noDefault()
          .endRecord();

  public static File generateAvroFile(int numRecords) throws IOException {
    File file = tmpFolder.newFile();
    List<GenericRecord> records = generateGenericRecords(numRecords);
    DatumWriter<GenericRecord> writer = new GenericDatumWriter<>(schema);
    try (DataFileWriter<GenericRecord> fileWriter = new DataFileWriter<>(writer)) {
      fileWriter.create(schema, file);
      for (GenericRecord record : records) {
        fileWriter.append(record);
      }
    }
    return file;
  }

  public static List<GenericRecord> generateGenericRecords(long count) {
    ArrayList<GenericRecord> result = new ArrayList<>();
    GenericRecordBuilder builder = new GenericRecordBuilder(schema);
    for (int i = 0; i < count; i++) {
      GenericRecord record = builder.set("user", "abcd").set("message", "Message #" + i).build();
      result.add(record);
    }
    return result;
  }

  public static List<Table.Row> generateDLPRows(long count) {
    ArrayList<Table.Row> result = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      Table.Row.Builder rowBuilder = Table.Row.newBuilder();
      Value user = Value.newBuilder().setStringValue("abcd").build();
      Value message = Value.newBuilder().setStringValue("Message #" + i).build();
      Table.Row row = rowBuilder.addValues(user).addValues(message).build();
      result.add(row);
    }
    return result;
  }

  @Test
  public void testAvroReaderSplitDoFn() throws IOException {
    int numRecords = 10;
    int numSplits = 5;
    int keyRange = 50;

    File file = generateAvroFile(numRecords);
    int fileSize = (int) Files.size(file.toPath());
    int splitSize = fileSize / numSplits;

    List<GenericRecord> records = generateGenericRecords(numRecords);

    PCollection<GenericRecord> results =
        pipeline
            .apply(FileIO.match().filepattern(file.getAbsolutePath()))
            .apply(FileIO.readMatches().withCompression(Compression.AUTO))
            .apply(WithKeys.of("some_key"))
            .apply(ParDo.of(new AvroReaderSplittableDoFn(keyRange, splitSize)))
            .setCoder(KvCoder.of(StringUtf8Coder.of(), AvroCoder.of(schema)))
            .apply(Values.create());

    PAssert.that(results).containsInAnyOrder(records);

    pipeline.run();
  }
}
