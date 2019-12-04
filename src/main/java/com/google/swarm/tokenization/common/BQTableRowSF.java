/*
 * Copyright 2018 Google LLC
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
import org.apache.beam.sdk.transforms.SimpleFunction;

public class BQTableRowSF extends SimpleFunction<Row, TableRow> {

  /** */
  private static final long serialVersionUID = 2479245991241032341L;

  @Override
  public TableRow apply(Row row) {

    TableRow bqRow = new TableRow();
    String[] headers = row.getHeader();
    String[] values = row.getValue();
    for (int i = 0; i < values.length; i++) {
      bqRow.set(headers[i], values[i]);
    }
    return bqRow;
  }
}
