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
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.fs.EmptyMatchTreatment;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTagList;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.io.fs.MoveOptions;
import org.apache.beam.sdk.io.fs.ResourceId;
import com.google.common.collect.ImmutableList;

@AutoValue
public abstract class FileReaderTransform extends PTransform<PBegin, PCollectionTuple> {

  public static final Logger LOG = LoggerFactory.getLogger(FileReaderTransform.class);

  public abstract String subscriber();

    public abstract Integer batchSize();

    @AutoValue.Builder
    public abstract static class Builder {
        public abstract Builder setSubscriber(String subscriber);

        public abstract Builder setBatchSize(Integer batchSize);

        public abstract FileReaderTransform build();
    }

  public static Builder newBuilder() {
    return new AutoValue_FileReaderTransform.Builder();
  }

  @Override
  public PCollectionTuple expand(PBegin input) {

    return input
        .apply(
            "ReadFileMetadata",
            PubsubIO.readMessagesWithAttributes().fromSubscription(subscriber()))
        .apply("ConvertToGCSUri", ParDo.of(new MapPubSubMessage()))
        .apply("FindFile", FileIO.matchAll().withEmptyMatchTreatment(EmptyMatchTreatment.ALLOW))
        .apply(FileIO.readMatches())
        .apply("AddFileNameAsKey", ParDo.of(new FileSourceDoFn()))
        .apply("ReadFile", ParDo.of(new FileReaderSplitDoFn(batchSize()))
                .withOutputTags(Util.readRowSuccess, TupleTagList.of(Util.readRowFailure)));
  }

  public class MapPubSubMessage extends DoFn<PubsubMessage, String> {

    private final Counter numberOfFilesReceived =
            Metrics.counter(FileReaderTransform.MapPubSubMessage.class, "NumberOfFilesReceived");

    @ProcessElement
    public void processElement(ProcessContext c) {

      LOG.info("File Read Transform:ConvertToGCSUri: Located File's Metadata : {} ", c.element().getAttributeMap());
      numberOfFilesReceived.inc(1L);

      String bucket = c.element().getAttribute("bucketId");
      String object = c.element().getAttribute("objectId");
      String eventType = c.element().getAttribute("eventType");
      String file_ts_string = c.element().getAttribute("eventTime");
      GcsPath uri = GcsPath.fromComponents(bucket, object);

      String file_name = uri.toString();
      String prefix;

      //Match filenames having extensions
      Matcher m1 = Pattern.compile("^gs://([^/]+)/(.*)\\.(.*)$").matcher(file_name);

            if (m1.find()) {
                prefix = m1.group(2);
            } else {//No extension
                prefix = object;
            }

            ImmutableList.Builder<ResourceId> sourceFiles = ImmutableList.builder();
            AtomicBoolean should_scan = new AtomicBoolean(true);

                if (!file_name.matches(Util.VALID_FILE_PATTERNS)) {
                    LOG.warn("File Read Transform:ConvertToGCSUri: Unsupported File Format. Skipping: {}", file_name);
                    should_scan.set(false);
                } else if (!eventType.equalsIgnoreCase(Util.ALLOWED_NOTIFICATION_EVENT_TYPE)) {
                    LOG.warn("File Read Transform:ConvertToGCSUri: Event Type Not Supported: {}. Skipping: {}", eventType,file_name);
                    should_scan.set(false);
                } else {
                    try {
                        MatchResult listResult = FileSystems.match("gs://" + bucket + "/" + prefix + ".*.dlp", EmptyMatchTreatment.ALLOW);
                        listResult.metadata().forEach(metadata -> {
                            ResourceId resourceId = metadata.resourceId();
                            Instant file_ts = Instant.parse(file_ts_string);
                            Instant tf_ts = new Instant(metadata.lastModifiedMillis());
                            if (resourceId.toString().equals("gs://" + bucket + "/" + prefix + ".rdct.dlp") && file_ts.isBefore(tf_ts)) {
                                LOG.warn("File Read Transform:ConvertToGCSUri: File has already been redacted. Skipping: {}", file_name);
                                should_scan.set(false);
                            } else {
                                LOG.warn("File Read Transform:ConvertToGCSUri: Deleting old touchfile: {}", resourceId.toString());
                                sourceFiles.add(resourceId);
                            }
                        });
                        FileSystems.delete(sourceFiles.build(), MoveOptions.StandardMoveOptions.IGNORE_MISSING_FILES);
                    } catch (IOException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
                }

                if (should_scan.get()) {
                    LOG.info("File Read Transform:ConvertToGCSUri: Valid File Located: {}", file_name);
                    c.output(file_name);
                }
        }
    }
}
