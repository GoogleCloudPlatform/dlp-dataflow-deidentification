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
package com.google.swarm.tokenization.beam;

import com.google.privacy.dlp.v2.Table;
import com.google.privacy.dlp.v2.Value;
import com.google.swarm.tokenization.common.Util;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Maps {@link KV}s of {@link String}s into KV<{@link String}, {@link Table.Row}> for further
 * processing in the DLP transforms.
 *
 * <p>If a column delimiter of values isn't provided, input is assumed to be unstructured and the
 * input KV value is saved in a single column of output {@link Table.Row}.
 */
public class ConvertCSVRecordToDLPRow extends DoFn<KV<String, String>, KV<String, Table.Row>> {

  public static final Logger LOG = LoggerFactory.getLogger(ConvertCSVRecordToDLPRow.class);

  private final Character columnDelimiter;
  private PCollectionView<Map<String, List<String>>> header;

  public ConvertCSVRecordToDLPRow(
      Character columnDelimiter, PCollectionView<Map<String, List<String>>> header) {
    this.columnDelimiter = columnDelimiter;
    this.header = header;
  }

  @ProcessElement
  public void processElement(ProcessContext context) throws IOException {
    Table.Row.Builder rowBuilder = Table.Row.newBuilder();
    String input = Objects.requireNonNull(context.element().getValue());
    String fileName = context.element().getKey();
    Map<String, List<String>> headers = context.sideInput(header);
    List<String> csvHeader = headers.get(fileName);
    if (csvHeader == null) {
      throw new RuntimeException(
          "Unable to find header row for fileName: "
              + fileName
              + ". The side input only contains header for "
              + headers.keySet());
    }

    if (columnDelimiter != null) {

      List<String> values = Util.parseLine(input, columnDelimiter, '"');
      if (values.size() == csvHeader.size()) {
        values.forEach(
            value -> rowBuilder.addValues(Value.newBuilder().setStringValue(value).build()));
        context.output(KV.of(fileName, rowBuilder.build()));

      } else {
        LOG.warn(
            "Rows in {} must have the same number of items {} as there are headers {}",
            fileName,
            values.size(),
            csvHeader.size());
      }
    } else {
      rowBuilder.addValues(Value.newBuilder().s etStringValue(input).build());
      context.output(KV.of(fileName, rowBuilder.build()));
    }
  }
}
