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

import java.io.IOException;
import java.util.List;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FilterBadRecordsDoFn extends DoFn<KV<String, String>, KV<String, String>> {

  public static final Logger LOG = LoggerFactory.getLogger(FilterBadRecordsDoFn.class);

  private final TupleTag<KV<String, String>> goodRecords;
  private final TupleTag<KV<String, String>> badRecords;

  private final PCollectionView<List<String>> csvHeader;

  private final Counter numberOfBadRecords =
      Metrics.counter(FilterBadRecordsDoFn.class, "NumberOfBadRecords");

  public FilterBadRecordsDoFn(
      TupleTag<KV<String, String>> goodRecords,
      TupleTag<KV<String, String>> badRecords,
      PCollectionView<List<String>> csvHeader) {
    this.goodRecords = goodRecords;
    this.badRecords = badRecords;
    this.csvHeader = csvHeader;
  }

  @ProcessElement
  public void processElement(
      @Element KV<String, String> element, MultiOutputReceiver out, ProcessContext c) {

    CSVFormat csvFormat = CSVFormat.DEFAULT;
    try {
      CSVParser parser = CSVParser.parse(element.getValue(), csvFormat);

      if (parser.getRecords().get(0).size() == c.sideInput(csvHeader).size()) {
        out.get(goodRecords).output(element);
      } else {
        out.get(badRecords).output(element);
        numberOfBadRecords.inc();
      }

    } catch (IOException e) {

      LOG.error("Failed to parse element: {}", e.getMessage());

      LOG.error("Bad record: {}", element.getValue());
      out.get(badRecords).output(element);
      numberOfBadRecords.inc();
    }
  }
}
