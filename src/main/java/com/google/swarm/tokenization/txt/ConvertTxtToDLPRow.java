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
package com.google.swarm.tokenization.txt;

import com.google.privacy.dlp.v2.Table;
import com.google.privacy.dlp.v2.Value;
import com.google.swarm.tokenization.beam.ConvertCSVRecordToDLPRow;
import com.google.swarm.tokenization.common.Util;
import java.io.IOException;
import java.util.List;
import java.util.Objects;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.ProcessContext;
import org.apache.beam.sdk.transforms.DoFn.ProcessElement;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("serial")
public class ConvertTxtToDLPRow extends DoFn<KV<String, String>, KV<String, Table.Row>> {

  public static final Logger LOG = LoggerFactory.getLogger(ConvertCSVRecordToDLPRow.class);

  private final Character columnDelimiter;
  private PCollectionView<List<String>> header;

  public ConvertTxtToDLPRow(PCollectionView<List<String>> header) {
    this.columnDelimiter = null;
    this.header = header;
  }

  public ConvertTxtToDLPRow(Character columnDelimiter, PCollectionView<List<String>> header) {
    this.columnDelimiter = columnDelimiter;
    this.header = header;
  }

  @ProcessElement
  public void processElement(ProcessContext context) throws IOException {
    Table.Row.Builder rowBuilder = Table.Row.newBuilder();
    String input = Objects.requireNonNull(context.element().getValue());
    List<String> csvHeader = context.sideInput(header);

    if (columnDelimiter != null) {

      List<String> values = Util.parseLine(input, columnDelimiter, '"');
      if (values.size() == csvHeader.size()) {
        values.forEach(
            value -> rowBuilder.addValues(Value.newBuilder().setStringValue(value).build()));
        context.output(KV.of(context.element().getKey(), rowBuilder.build()));

      } else {
        LOG.warn(
            "Rows must have the same number of items {} as there are headers {}",
            values.size(),
            csvHeader.size());
      }
    } else {
      rowBuilder.addValues(Value.newBuilder().setStringValue(input).build());
      context.output(KV.of(context.element().getKey(), rowBuilder.build()));
    }
  }
}
