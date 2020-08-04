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

import com.google.swarm.tokenization.common.Util.FileType;
import com.google.swarm.tokenization.common.Util.DLPMethod;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.io.aws.options.S3Options;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.TypedRead.Method;
import org.apache.beam.sdk.options.*;

public interface DLPTextToBigQueryStreamingV2PipelineOptions
    extends DataflowPipelineOptions, S3Options {

  @Validation.Required
  String getFilePattern();

  void setFilePattern(String csvFilePattern);

  @Description("DLP Inspect Template Name")
  String getInspectTemplateName();

  void setInspectTemplateName(String value);

  @Description("DLP DeIdentify Template Name")
  String getDeidentifyTemplateName();

  void setDeidentifyTemplateName(String value);

  @Description("DLP method deid,inspect,reid")
  @Default.Enum("INSPECT")
  DLPMethod getDLPMethod();

  void setDLPMethod(DLPMethod value);

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

  @Description("BigQuery table to export from in the form <project>:<dataset>.<table>")
  String getTableRef();

  void setTableRef(String tableRef);

  @Description("read method default, direct, export")
  @Default.Enum("EXPORT")
  Method getReadMethod();

  void setReadMethod(Method method);

  @Description("Query")
  String getQueryPath();

  void setQueryPath(String topic);

  @Description("Topic to use for reid result")
  String getTopic();

  void setTopic(String topic);

  @Default.Long(500 * 1000)
  Long getAvroMaxBytesPerSplit();

  void setAvroMaxBytesPerSplit(Long value);

  @Default.Long(50 * 1000)
  Long getAvroMaxCellsPerSplit();

  void setAvroMaxCellsPerSplit(Long value);

  class FileTypeFactory implements DefaultValueFactory<FileType> {
    @Override
    public FileType create(PipelineOptions options) {
      if (((DLPTextToBigQueryStreamingV2PipelineOptions) options).getFilePattern().toLowerCase().endsWith(".avro")) {
        return FileType.AVRO;
      }
      else {
        return FileType.CSV;
      }
    }
  }

  @Validation.Required
  @Default.InstanceFactory(FileTypeFactory.class)
  FileType getFileType();

  void setFileType(FileType fileType);

}
