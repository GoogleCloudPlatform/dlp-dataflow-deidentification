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
import org.apache.beam.sdk.io.FileIO.ReadableFile;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CSVFileHeaderDoFn extends DoFn<KV<String, ReadableFile>, String> {
  public static final Logger LOG = LoggerFactory.getLogger(CSVFileHeaderDoFn.class);

  @ProcessElement
  public void processElement(ProcessContext c) {
    ReadableFile file = c.element().getValue();
    try (BufferedReader br = Util.getReader(file)) {
      CSVRecord csvHeader = CSVFormat.DEFAULT.parse(br).getRecords().get(0);
      csvHeader.forEach(
          headerValue -> {
            c.output(headerValue);
            LOG.info("header value {}", headerValue);
          });
    } catch (IOException e) {
      LOG.error("Failed to get csv header values}", e.getMessage());
      throw new RuntimeException(e);
    }
  }
}
