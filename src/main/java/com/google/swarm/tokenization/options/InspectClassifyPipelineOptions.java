/*
 * Copyright 2023 Google LLC
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
package com.google.swarm.tokenization.options;

import com.google.swarm.tokenization.common.Util;
import java.util.List;
import org.apache.beam.sdk.options.*;

public interface InspectClassifyPipelineOptions extends CommonPipelineOptions {
  @Validation.Required
  String getFilePattern();

  void setFilePattern(String csvFilePattern);

  @Description("DLP method deid,inspect,reid")
  @Default.Enum("INSPECT")
  Util.DLPMethod getDLPMethod();

  void setDLPMethod(Util.DLPMethod value);

  @Description("BQ Dataset")
  String getDataset();

  void setDataset(String value);

  List<String> getFileTypes();

  void setFileTypes(List<String> fileTypes);

  @Description("Topic to publish the inspection results")
  String getTopic();

  void setTopic(String topic);

  @Description("Column Delimiter for csv files")
  @Default.Character(',')
  Character getColumnDelimiterForCsvFiles();

  void setColumnDelimiterForCsvFiles(Character value);

  @Description("Record delimiter")
  @Default.String("\n")
  String getRecordDelimiter();

  void setRecordDelimiter(String value);

  @Description("DLP Table Headers- Required for Jsonl type")
  List<String> getHeaders();

  void setHeaders(List<String> topic);
}
