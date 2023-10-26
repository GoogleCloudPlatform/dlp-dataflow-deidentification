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
package com.google.swarm.tokenization;

import com.google.privacy.dlp.v2.LocationName;
import com.google.swarm.tokenization.common.Util;
import java.util.List;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.io.aws.options.S3Options;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.*;

public interface InspectClassifyPipelineOptions extends DataflowPipelineOptions, S3Options {

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
  Util.DLPMethod getDLPMethod();

  void setDLPMethod(Util.DLPMethod value);

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

  @Description("Record delimiter")
  @Default.String("\n")
  String getRecordDelimiter();

  void setRecordDelimiter(String value);

  @Description("Column delimiter")
  @Default.Character(',')
  Character getColumnDelimiter();

  void setColumnDelimiter(Character value);

  @Description("BigQuery table to export from in the form <project>:<dataset>.<table>")
  String getTableRef();

  void setTableRef(String tableRef);

  @Description("read method default, direct, export")
  @Default.Enum("EXPORT")
  BigQueryIO.TypedRead.Method getReadMethod();

  void setReadMethod(BigQueryIO.TypedRead.Method method);

  @Description("Query")
  String getQueryPath();

  void setQueryPath(String topic);

  @Description("Topic to use for reid result")
  String getTopic();

  void setTopic(String topic);

  @Default.Integer(900 * 1000)
  Integer getSplitSize();

  void setSplitSize(Integer value);

  @Description("DLP Table Headers- Required for Json type")
  List<String> getHeaders();

  void setHeaders(List<String> topic);

  @Description("Input file schema- Required for ORC type")
  String getSchema();

  void setSchema(String schema);

  @Description(
      "Number of shards for DLP request batches. "
          + "Can be used to controls parallelism of DLP requests.")
  @Default.Integer(100)
  int getNumShardsPerDLPRequestBatching();

  void setNumShardsPerDLPRequestBatching(int value);

  @Description("Number of retries in case of transient errors in DLP API")
  @Default.Integer(10)
  int getDlpApiRetryCount();

  void setDlpApiRetryCount(int value);

  @Description("Initial backoff (in seconds) for retries with exponential backoff")
  @Default.Integer(5)
  int getInitialBackoff();

  /**
   * Initial backoff (in seconds) for retries with exponential backoff. See {@link
   * org.apache.beam.sdk.util.FluentBackoff.BackoffImpl#nextBackOffMillis()} for details on how the
   * exponential backoff is implemented.
   */
  void setInitialBackoff(int value);

  @Description("Output bucket to write DEID output as csv file")
  String getOutputBucket();

  void setOutputBucket(String outputBucket);

  class DataSinkFactory implements DefaultValueFactory<Util.DataSinkType> {
    @Override
    public Util.DataSinkType create(PipelineOptions options) {
      if (((InspectClassifyPipelineOptions) options).getOutputBucket() != null)
        return Util.DataSinkType.GCS;
      else return Util.DataSinkType.BigQuery;
    }
  }

  @Validation.Required
  @Default.InstanceFactory(DataSinkFactory.class)
  Util.DataSinkType getDataSinkType();

  void setDataSinkType(Util.DataSinkType dataSinkType);

  class FileTypeFactory implements DefaultValueFactory<Util.FileType> {
    @Override
    public Util.FileType create(PipelineOptions options) {
      if (((InspectClassifyPipelineOptions) options)
          .getFilePattern()
          .toLowerCase()
          .endsWith(".avro")) {
        return Util.FileType.AVRO;

      } else if (((InspectClassifyPipelineOptions) options)
          .getFilePattern()
          .toLowerCase()
          .endsWith(".jsonl")) {
        return Util.FileType.JSONL;
      } else if (((InspectClassifyPipelineOptions) options)
          .getFilePattern()
          .toLowerCase()
          .endsWith(".txt")) {
        return Util.FileType.TXT;
      } else if (((InspectClassifyPipelineOptions) options)
          .getFilePattern()
          .toLowerCase()
          .endsWith(".tsv")) {
        return Util.FileType.TSV;
      } else if (((InspectClassifyPipelineOptions) options)
          .getFilePattern()
          .toLowerCase()
          .endsWith(".parquet")) {
        return Util.FileType.PARQUET;
      } else if (((InspectClassifyPipelineOptions) options)
          .getFilePattern()
          .toLowerCase()
          .endsWith(".orc")) {
        return Util.FileType.ORC;
      } else {
        return Util.FileType.CSV;
      }
    }
  }

  @Validation.Required
  @Default.InstanceFactory(FileTypeFactory.class)
  Util.FileType getFileType();

  void setFileType(Util.FileType fileType);

  List<String> getFileTypes();

  void setFileTypes(List<String> fileTypes);

  class DLPConfigProjectFactory implements DefaultValueFactory<String> {
    @Override
    public String create(PipelineOptions options) {
      return LocationName.of(
              ((InspectClassifyPipelineOptions) options).getProject(), "global")
          .toString();
    }
  }

  @Default.InstanceFactory(DLPConfigProjectFactory.class)
  String getDLPParent();

  void setDLPParent(String parent);

  class InputPollingFactory implements DefaultValueFactory<Util.InputLocation> {
    @Override
    public Util.InputLocation create(PipelineOptions options) {
      if (((InspectClassifyPipelineOptions) options)
          .getFilePattern()
          .startsWith("gs://")) return Util.InputLocation.GCS;
      else return Util.InputLocation.NOT_GCS;
    }
  }

  @Validation.Required
  @Default.InstanceFactory(InputPollingFactory.class)
  Util.InputLocation getInputProviderType();

  void setInputProviderType(Util.InputLocation input);

  @Description("Topic to use for GCS Pub/Sub")
  String getGcsNotificationTopic();

  void setGcsNotificationTopic(String topic);

  @Description("Flag to process existing files")
  @Default.Boolean(true)
  Boolean getProcessExistingFiles();

  void setProcessExistingFiles(Boolean value);
}
