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
package com.google.swarm.tokenization.common;

import com.google.auto.value.AutoValue;
import com.google.swarm.tokenization.common.Util.InputLocation;
import javax.annotation.Nullable;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.FileIO.ReadableFile;
import org.apache.beam.sdk.io.ReadableFileCoder;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;

/** Transform that polls for new files and sanitizes file names. */
@AutoValue
public abstract class ReadNewFilesPubSubTransform
    extends PTransform<PBegin, PCollection<KV<String, ReadableFile>>> {

  public abstract String filePattern();

  @Nullable
  public abstract String pubSubTopic();

  public abstract Boolean usePubSub();

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract ReadNewFilesPubSubTransform.Builder setFilePattern(String filePattern);

    public abstract ReadNewFilesPubSubTransform.Builder setPubSubTopic(String pubSubTopic);

    public abstract ReadNewFilesPubSubTransform.Builder setUsePubSub(Boolean usePubSub);

    public abstract ReadNewFilesPubSubTransform build();
  }

  public static ReadNewFilesPubSubTransform.Builder newBuilder() {
    return new AutoValue_ReadNewFilesPubSubTransform.Builder();
  }

  @Override
  public PCollection<KV<String, ReadableFile>> expand(PBegin input) {
    if (!usePubSub()) {
      return input.apply(
          "CreateEmptyNewFileSet",
          Create.empty(KvCoder.of(StringUtf8Coder.of(), ReadableFileCoder.of())));
    }
    return input
        .apply("ReadFromTopic", PubsubIO.readMessagesWithAttributes().fromTopic(pubSubTopic()))
        .apply(
            "ReadFileMetadataFromMessage", ParDo.of(new PubSubReadFileMetadataDoFn(filePattern())))
        .apply("ReadNewFiles", FileIO.readMatches())
        .apply("SanitizeFileNameNewFiles", ParDo.of(new SanitizeFileNameDoFn(InputLocation.GCS)));
  }
}
