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
import com.google.swarm.tokenization.common.Util.InputLocation;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.FileIO.ReadableFile;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Watch;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;

/** Transform that polls for new files and sanitizes file names. */
@AutoValue
public abstract class FilePollingTransform
    extends PTransform<PBegin, PCollection<KV<String, ReadableFile>>> {

  public abstract String filePattern();

  public abstract Duration interval();

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract FilePollingTransform.Builder setFilePattern(String filePattern);

    public abstract FilePollingTransform.Builder setInterval(Duration interval);

    public abstract FilePollingTransform build();
  }

  public static FilePollingTransform.Builder newBuilder() {
    return new AutoValue_FilePollingTransform.Builder();
  }

  @Override
  public PCollection<KV<String, ReadableFile>> expand(PBegin input) {
    return input
        .apply(
            "Poll Input Files",
            FileIO.match()
                .filepattern(filePattern())
                .continuously(interval(), Watch.Growth.never()))
        .apply("Find Pattern Match", FileIO.readMatches().withCompression(Compression.AUTO))
        .apply(ParDo.of(new SanitizeFileNameDoFn(InputLocation.S3)));
  }
}
