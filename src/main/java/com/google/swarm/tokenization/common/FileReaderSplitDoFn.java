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
package com.google.swarm.tokenization.common;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.Iterator;
import java.util.Random;
import org.apache.beam.sdk.io.FileIO.ReadableFile;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("serial")
public class FileReaderSplitDoFn extends DoFn<KV<String, ReadableFile>, KV<String, CSVRecord>> {
  public static final Logger LOG = LoggerFactory.getLogger(FileReaderSplitDoFn.class);
  private final Counter numberOfRowsRead =
      Metrics.counter(FileReaderSplitDoFn.class, "numberOfRowsRead");

  private Integer keyRange;

  public FileReaderSplitDoFn(Integer keyRange) {
    this.keyRange = keyRange;
  }

  @ProcessElement
  public void processElement(ProcessContext c) throws IOException {
    String fileName = c.element().getKey();
    try (BufferedReader br = Util.getReader(c.element().getValue())) {
      Iterator<CSVRecord> csvRows = CSVFormat.DEFAULT.withFirstRecordAsHeader()
    		  .withSkipHeaderRecord().parse(br).iterator();

      while (csvRows.hasNext()) {
        String key = String.format("%s~%d", fileName, new Random().nextInt(keyRange));
        numberOfRowsRead.inc();
        c.outputWithTimestamp(KV.of(key, csvRows.next()), Instant.now());
      }
    }
  }
}
