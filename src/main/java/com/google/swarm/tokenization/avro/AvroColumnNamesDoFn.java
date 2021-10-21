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
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.beam.sdk.io.FileIO.ReadableFile;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Reads the given avro file's schema then outputs flattened column names. */
public class AvroColumnNamesDoFn extends DoFn<KV<String, ReadableFile>, KV<String,List<String>>> {

  public static final Logger LOG = LoggerFactory.getLogger(AvroColumnNamesDoFn.class);

  @ProcessElement
  public void processElement(ProcessContext c) {
    ReadableFile avroFile = c.element().getValue();
    try (AvroUtil.AvroSeekableByteChannel channel = AvroUtil.getChannel(avroFile)) {
      DatumReader<GenericRecord> reader = new GenericDatumReader<>();
      DataFileReader<GenericRecord> fileReader = new DataFileReader<>(channel, reader);
      List<String> fieldNames = new ArrayList<>();
      AvroUtil.flattenFieldNames(fileReader.getSchema(), fieldNames, "");

      String fileName = c.element().getKey();
      c.output(KV.of(fileName, fieldNames));

      LOG.info("Avro header fields: {}", String.join(",", fieldNames));
    } catch (IOException e) {
      LOG.error("Failed to get Avro header values: {}", e.getMessage());
      throw new RuntimeException(e);
    }
  }
}
