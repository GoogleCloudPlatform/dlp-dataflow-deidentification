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
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.ReadableFileCoder;
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
public abstract class S3ReaderTransform
    extends PTransform<PBegin, PCollection<KV<String, String>>> {
  public static final Logger LOG = LoggerFactory.getLogger(S3ReaderTransform.class);

  public abstract String csvFilePattern();

  public abstract Duration pollInterval();

  public abstract String delimeter();

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder setCsvFilePattern(String csvFilePattern);

    public abstract Builder setDelimeter(String delimeter);

    public abstract Builder setPollInterval(Duration pollInterval);

    public abstract S3ReaderTransform build();
  }

  public static Builder newBuilder() {
    return new AutoValue_S3ReaderTransform.Builder();
  }

  @Override
  public PCollection<KV<String, String>> expand(PBegin input) {
    return input
        .apply(
            "Poll Input CSV Files",
            FileIO.match()
                .filepattern(csvFilePattern())
                .continuously(pollInterval(), Watch.Growth.never()))
        .apply("Find Pattern Match", FileIO.readMatches().withCompression(Compression.AUTO))
        .apply("Add File Name as Key", ParDo.of(new FileSourceDoFn()))
        .setCoder(KvCoder.of(StringUtf8Coder.of(), ReadableFileCoder.of()))
        .apply("Read File", ParDo.of(new FileReaderSplitDoFn(delimeter())));
  }
}
