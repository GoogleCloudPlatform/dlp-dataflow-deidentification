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
package com.google.swarm.tokenization.options;

import com.google.privacy.dlp.v2.LocationName;
import com.google.swarm.tokenization.common.Util;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.io.aws.options.S3Options;
import org.apache.beam.sdk.options.*;

public interface CommonPipelineOptions extends DataflowPipelineOptions, S3Options {

  @Description("DLP Inspect Template Name")
  String getInspectTemplateName();

  void setInspectTemplateName(String value);

  @Description("DLP DeIdentify Template Name")
  String getDeidentifyTemplateName();

  void setDeidentifyTemplateName(String value);

  @Description("Batch Size (max 524kb)")
  @Default.Integer(500000)
  Integer getBatchSize();

  void setBatchSize(Integer value);

  @Description("key range")
  @Default.Integer(100)
  Integer getKeyRange();

  void setKeyRange(Integer value);

  @Default.Integer(900 * 1000)
  Integer getSplitSize();

  void setSplitSize(Integer value);

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

  class DLPConfigProjectFactory implements DefaultValueFactory<String> {
    @Override
    public String create(PipelineOptions options) {
      return LocationName.of(
              ((com.google.swarm.tokenization.options.CommonPipelineOptions) options).getProject(),
              "global")
          .toString();
    }
  }

  @Default.InstanceFactory(DLPConfigProjectFactory.class)
  String getDLPParent();

  void setDLPParent(String parent);

  class InputPollingFactory implements DefaultValueFactory<Util.InputLocation> {
    @Override
    public Util.InputLocation create(PipelineOptions options) {
      if (((com.google.swarm.tokenization.DLPTextToBigQueryStreamingV2PipelineOptions) options)
          .getFilePattern()
          .startsWith("gs://")) return Util.InputLocation.GCS;
      else return Util.InputLocation.NOT_GCS;
    }
  }

  @Validation.Required
  @Default.InstanceFactory(
      com.google.swarm.tokenization.DLPTextToBigQueryStreamingV2PipelineOptions.InputPollingFactory
          .class)
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
