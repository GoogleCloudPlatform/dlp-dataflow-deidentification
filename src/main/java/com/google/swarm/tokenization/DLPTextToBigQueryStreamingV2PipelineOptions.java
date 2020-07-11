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

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.io.aws.options.S3Options;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.Validation;

public interface DLPTextToBigQueryStreamingV2PipelineOptions
    extends DataflowPipelineOptions, S3Options {
  @Validation.Required
  String getCSVFilePattern();

  void setCSVFilePattern(String csvFilePattern);

  @Description("DLP Inspect Template Name")
  String getInspectTemplateName();

  void setInspectTemplateName(String value);

  @Description("DLP DeIdentify Template Name")
  String getDeidTemplateName();

  void setDeidTemplateName(String value);

  @Description("DLP method deid,inspect,reid")
  @Default.String("inspect")
  String getDLPMethod();

  void setDLPMethod(String value);

  @Description("Batch Size (max 524kb)")
  @Default.Integer(500000)
  Integer getBatchSize();

  void setBatchSize(Integer value);

  @Description("key range")
  @Default.Integer(100)
  Integer getKeyRange();

  void setKeyRange(Integer value);

  @Description("BQ Dataset")
  String getDataset();

  void setDataset(String value);

  @Description("BQ Write Method")
  @Default.Enum("DEFAULT")
  BigQueryIO.Write.Method getWriteMethod();

  void setWriteMethod(BigQueryIO.Write.Method value);

  @Description("Line delimeter")
  @Default.String("\n")
  String getDelimeter();

  void setDelimeter(String value);

  @Description("Column delimeter")
  @Default.String(",")
  String getColumnDelimeter();

  void setColumnDelimeter(String value);

  @Description("Run mode S3, default(gcs)")
  @Default.String("default")
  String getRunMode();

  void setRunMode(String value);
}
