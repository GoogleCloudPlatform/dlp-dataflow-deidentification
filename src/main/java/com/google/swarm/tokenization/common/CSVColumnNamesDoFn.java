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
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.io.FileIO.ReadableFile;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.commons.csv.CSVFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("serial")
public class CSVColumnNamesDoFn extends DoFn<KV<String, ReadableFile>, KV<String, List<String>>> {

  public static final Logger LOG = LoggerFactory.getLogger(CSVColumnNamesDoFn.class);

  @ProcessElement
  public void processElement(ProcessContext c) {
    ReadableFile file = c.element().getValue();
    try (BufferedReader br = Util.getReader(file)) {

      List<String> columnNames = new ArrayList<>();

      CSVFormat.DEFAULT
          .withFirstRecordAsHeader()
          .parse(br)
          .getHeaderMap()
          .keySet()
          .forEach(
              (key) -> {
                columnNames.add(key);
              });

      String fileName = c.element().getKey();
      c.output(KV.of(fileName, columnNames));

    } catch (IOException e) {
      LOG.error("Failed to get csv header values. Error message: {}", e.getMessage());
      throw new RuntimeException(e);
    }
  }
}
