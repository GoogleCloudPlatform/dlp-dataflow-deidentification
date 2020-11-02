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
import java.io.IOException;
import java.util.List;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Maps {@link KV}s of {@link String}s into KV<{@link String}, {@link Table.Row}> for further
 * processing in the DLP transforms.
 *
 * <p>If a column delimiter of values isn't provided, input is assumed to be unstructured and the
 * input KV value is saved in a single column of output {@link Table.Row}.
 */
public class ConvertCSVRecordToDLPRow extends DoFn<Row, KV<String, Table.Row>> {

  public static final Logger LOG = LoggerFactory.getLogger(ConvertCSVRecordToDLPRow.class);

  private final Character columnDelimiter;
  private PCollectionView<List<String>> header;

  public ConvertCSVRecordToDLPRow(PCollectionView<List<String>> header) {
    this.columnDelimiter = null;
    this.header = header;
  }

  public ConvertCSVRecordToDLPRow(Character columnDelimiter, PCollectionView<List<String>> header) {
    this.columnDelimiter = columnDelimiter;
    this.header = header;
  }

  @ProcessElement
  public void processElement(ProcessContext context) throws IOException {
    Table.Row.Builder rowBuilder = Table.Row.newBuilder();
    LOG.info("Row {}", context.element().toString());
    context.output(KV.of("test",rowBuilder.build()));
  }
}
