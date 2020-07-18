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
package com.google.swarm.tokenization;

import java.util.List;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.TypedRead.Method;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.Validation.Required;

public interface DLPBigQueryToPubSubReidPipelineOptions
    extends DLPTextToBigQueryStreamingV2PipelineOptions {
  @Description("BigQuery table to export from in the form <project>:<dataset>.<table>")
  @Required
  String getTableRef();

  void setTableRef(String tableRef);

  @Description("Optional: Comma separated list of fields to select from the table.")
  List<String> getFields();

  void setFields(List<String> fields);

  @Description("read method direct, export")
  Method getReadMethod();

  void setReadMethod(Method method);
}
