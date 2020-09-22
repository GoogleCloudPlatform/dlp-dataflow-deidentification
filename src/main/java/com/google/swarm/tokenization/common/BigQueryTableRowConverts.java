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
import java.io.ByteArrayInputStream;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("serial")
public class BigQueryTableRowConverts
    extends SimpleFunction<KV<String, String>, KV<String, TableRow>> {
  public static final Logger LOG = LoggerFactory.getLogger(BigQueryTableRowConverts.class);

  @Override
  public KV<String, TableRow> apply(KV<String, String> input) {

    String tableRef = input.getKey();
    TableRow tableRow = null;
    try {
      tableRow =
          TableRowJsonCoder.of()
          .decode(new ByteArrayInputStream(input.getValue().getBytes()));
    } catch (Exception e) {
      e.printStackTrace();
    }
    return KV.of(tableRef, tableRow);
  }
}
