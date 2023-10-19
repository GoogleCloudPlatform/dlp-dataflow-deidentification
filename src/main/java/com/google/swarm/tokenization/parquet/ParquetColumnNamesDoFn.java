/*
 * Copyright 2023 Google LLC
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
package com.google.swarm.tokenization.parquet;

import java.io.IOException;
import java.nio.channels.SeekableByteChannel;
import java.util.List;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.io.FileIO.ReadableFile;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Reads the given Parquet file's schema then outputs flattened column names. */
public class ParquetColumnNamesDoFn
    extends DoFn<KV<String, ReadableFile>, KV<String, List<String>>> {

  public static final Logger LOG = LoggerFactory.getLogger(ParquetColumnNamesDoFn.class);

  @ProcessElement
  public void processElement(ProcessContext c) throws IOException {
    LOG.info("Processing column names for parquet files");
    String fileName = c.element().getKey();
    ReadableFile parquetFile = c.element().getValue();

    SeekableByteChannel seekableByteChannel = parquetFile.openSeekable();
    AvroParquetReader.Builder builder =
        AvroParquetReader.<GenericRecord>builder(new BeamParquetInputFile(seekableByteChannel));

    try (ParquetReader<GenericRecord> fileReader = builder.build()) {
      GenericRecord record = fileReader.read();
      List<String> flattenedFieldNames = RecordFlattener.forGenericRecord().flattenColumns(record);
      c.output(KV.of(fileName, flattenedFieldNames));
    }
    seekableByteChannel.close();
  }
}
