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

import com.google.auto.value.AutoValue;
import com.google.swarm.tokenization.common.CSVFileReaderTransform.Builder;
import org.apache.beam.sdk.extensions.gcp.util.gcsfs.GcsPath;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Watch;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@AutoValue
public abstract class FileReaderTransform
    extends PTransform<PBegin, PCollection<KV<String, String>>> {

  public static final Logger LOG = LoggerFactory.getLogger(FileReaderTransform.class);

  public abstract String subscriber();

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder setSubscriber(String subscriber);

    public abstract FileReaderTransform build();
  }

  public static Builder newBuilder() {
    return new AutoValue_FileReaderTransform.Builder();
  }

  @Override
  public PCollection<KV<String, String>> expand(PBegin input) {
	  // gs://stress-test-buck/testing_data/*.dat
	  // gs://dlp_scan_run_test/daily_import_*.csv
    return input
        .apply(
            FileIO.match()
                .filepattern("gs://stress-test-buck/testing_data/student_data_*.dat")
                .continuously(
                    Duration.standardSeconds(30),
                    Watch.Growth.afterTimeSinceNewOutput(Duration.standardHours(1))))
        .apply("Find Pattern Match", FileIO.readMatches().withCompression(Compression.AUTO))
        .apply("AddFileNameAsKey", ParDo.of(new FileSourceDoFn()))
        .apply("ReadFile", ParDo.of(new FileReaderSplitDoFn()));
  }

  public class MapPubSubMessage extends DoFn<PubsubMessage, String> {

    @ProcessElement
    public void processElement(ProcessContext c) {
      String bucket = c.element().getAttribute("bucketId");
      String object = c.element().getAttribute("objectId");
      String eventType = c.element().getAttribute("eventType");
      GcsPath uri = GcsPath.fromComponents(bucket, object);

      if (eventType.equalsIgnoreCase(Util.ALLOWED_NOTIFICATION_EVENT_TYPE)) {
        LOG.info("File Name {}", uri.toString());
        c.output(uri.toString());
      } else {
        LOG.info("Event Type Not Supported {}", eventType);
      }
    }
  }
}
