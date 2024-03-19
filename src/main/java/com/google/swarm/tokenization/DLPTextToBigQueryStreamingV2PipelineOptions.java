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

import com.google.privacy.dlp.v2.LocationName;
import com.google.swarm.tokenization.common.Util.DLPMethod;
import com.google.swarm.tokenization.common.Util.DataSinkType;
import com.google.swarm.tokenization.common.Util.FileType;
import com.google.swarm.tokenization.common.Util.InputLocation;
import com.google.swarm.tokenization.options.CommonPipelineOptions;
import java.util.List;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.TypedRead.Method;
import org.apache.beam.sdk.options.*;

public interface DLPTextToBigQueryStreamingV2PipelineOptions extends CommonPipelineOptions {

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
  Method getReadMethod();

  void setReadMethod(Method method);

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

  @Description("Output bucket to write de-identified files in Cloud Storage buckets")
  String getOutputBucket();

  void setOutputBucket(String outputBucket);

  class DataSinkFactory implements DefaultValueFactory<DataSinkType> {
    @Override
    public DataSinkType create(PipelineOptions options) {
      if (((DLPTextToBigQueryStreamingV2PipelineOptions) options).getOutputBucket() != null)
        return DataSinkType.GCS;
      else return DataSinkType.BigQuery;
    }
  }

  @Validation.Required
  @Default.InstanceFactory(DataSinkFactory.class)
  DataSinkType getDataSinkType();

  void setDataSinkType(DataSinkType dataSinkType);

  class FileTypeFactory implements DefaultValueFactory<FileType> {
    @Override
    public FileType create(PipelineOptions options) {
      if (((DLPTextToBigQueryStreamingV2PipelineOptions) options)
          .getFilePattern()
          .toLowerCase()
          .endsWith(".avro")) {
        return FileType.AVRO;

      } else if (((DLPTextToBigQueryStreamingV2PipelineOptions) options)
          .getFilePattern()
          .toLowerCase()
          .endsWith(".jsonl")) {
        return FileType.JSONL;
      } else if (((DLPTextToBigQueryStreamingV2PipelineOptions) options)
          .getFilePattern()
          .toLowerCase()
          .endsWith(".txt")) {
        return FileType.TXT;
      } else if (((DLPTextToBigQueryStreamingV2PipelineOptions) options)
          .getFilePattern()
          .toLowerCase()
          .endsWith(".tsv")) {
        return FileType.TSV;
      } else if (((DLPTextToBigQueryStreamingV2PipelineOptions) options)
          .getFilePattern()
          .toLowerCase()
          .endsWith(".parquet")) {
        return FileType.PARQUET;
      } else if (((DLPTextToBigQueryStreamingV2PipelineOptions) options)
          .getFilePattern()
          .toLowerCase()
          .endsWith(".orc")) {
        return FileType.ORC;
      } else {
        return FileType.CSV;
      }
    }
  }

  @Validation.Required
  @Default.InstanceFactory(FileTypeFactory.class)
  FileType getFileType();

  void setFileType(FileType fileType);

  class DLPConfigProjectFactory implements DefaultValueFactory<String> {
    @Override
    public String create(PipelineOptions options) {
      return LocationName.of(
              ((DLPTextToBigQueryStreamingV2PipelineOptions) options).getProject(), "global")
          .toString();
    }
  }

  @Default.InstanceFactory(DLPConfigProjectFactory.class)
  String getDLPParent();

  void setDLPParent(String parent);

  class InputPollingFactory implements DefaultValueFactory<InputLocation> {
    @Override
    public InputLocation create(PipelineOptions options) {
      if (((DLPTextToBigQueryStreamingV2PipelineOptions) options)
          .getFilePattern()
          .startsWith("gs://")) return InputLocation.GCS;
      else return InputLocation.NOT_GCS;
    }
  }

  @Validation.Required
  @Default.InstanceFactory(InputPollingFactory.class)
  InputLocation getInputProviderType();

  void setInputProviderType(InputLocation input);

  @Description("Topic to use for GCS Pub/Sub")
  String getGcsNotificationTopic();

  void setGcsNotificationTopic(String topic);

  @Description("Flag to process existing files")
  @Default.Boolean(true)
  Boolean getProcessExistingFiles();

  void setProcessExistingFiles(Boolean value);
}
