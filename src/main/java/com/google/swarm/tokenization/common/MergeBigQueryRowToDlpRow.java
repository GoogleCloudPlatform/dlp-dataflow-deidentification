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

import com.google.api.services.bigquery.model.TableRow;
import com.google.privacy.dlp.v2.Table;
import com.google.privacy.dlp.v2.Value;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

public class MergeBigQueryRowToDlpRow extends DoFn<KV<String, TableRow>, KV<String, Table.Row>> {

  @ProcessElement
  public void processElement(ProcessContext c) {
    TableRow bigqueryRow = c.element().getValue();
    Table.Row.Builder rowBuilder = Table.Row.newBuilder();
    bigqueryRow
        .entrySet()
        .forEach(
            element -> {
              String value = element.getValue().toString();
              rowBuilder.addValues(Value.newBuilder().setStringValue(value).build());
            });
    c.output(KV.of(c.element().getKey(), rowBuilder.build()));
  }
}
