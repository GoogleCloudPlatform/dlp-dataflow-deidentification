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

import com.google.privacy.dlp.v2.Table;
import com.google.privacy.dlp.v2.Value;
import java.util.List;
import java.util.Objects;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("serial")
class MapStringToDlpRow extends DoFn<KV<String, String>, KV<String, Table.Row>> {

  public static final Logger LOG = LoggerFactory.getLogger(MapStringToDlpRow.class);

  private final String delimiter;
  private final PCollectionView<List<String>> headerColumns;

  private final Counter numberOfBadRecords =
      Metrics.counter(MapStringToDlpRow.class, "NumberOfBadRecords");

  public MapStringToDlpRow(String delimiter, PCollectionView<List<String>> headerColumns) {
    this.delimiter = delimiter;
    this.headerColumns = headerColumns;
  }

  @ProcessElement
  public void processElement(ProcessContext context) {
    Table.Row.Builder rowBuilder = Table.Row.newBuilder();
    String line = Objects.requireNonNull(context.element().getValue());

    if (delimiter != null) {
      List<String> values = Util.parseLine(line, delimiter.charAt(0), '"');
      values.forEach(value -> rowBuilder.addValues(Value.newBuilder().setStringValue(value)));
      //          LOG.info("Data: {}", values.toString());
      if (values.size() == context.sideInput(headerColumns).size()) {
        context.output(KV.of(context.element().getKey(), rowBuilder.build()));
      } else {
        numberOfBadRecords.inc();
      }
    } else {
      rowBuilder.addValues(Value.newBuilder().setStringValue(line));
    }

    //    // TODO: Add support for user supplied delimiter to build CSVFormat
    //    CSVFormat csvFormat = CSVFormat.DEFAULT;
    //    try {
    //      CSVParser parser = CSVParser.parse(line, csvFormat);
    //
    //      for (CSVRecord csvRecord : parser) {
    //
    //        csvRecord.forEach(
    //            r -> {
    //              rowBuilder.addValues(Value.newBuilder().setStringValue(r));
    //            });
    //      }
    //
    //    } catch (IOException e) {
    //      LOG.error("Bad element: {}", line);
    //    }

    //    if c.sideInput(csvHeader).size()
    //    context.output(KV.of(context.element().getKey(), rowBuilder.build()));
  }
}
