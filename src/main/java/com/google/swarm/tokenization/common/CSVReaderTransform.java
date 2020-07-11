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
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.FileIO.ReadableFile;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Watch;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("serial")
@AutoValue
public abstract class CSVReaderTransform extends PTransform<PBegin, PCollectionTuple> {
  public static final Logger LOG = LoggerFactory.getLogger(CSVReaderTransform.class);

  public abstract String delimeter();

  public abstract String filePattern();

  public abstract Duration interval();

  public abstract Integer keyRange();

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setDelimeter(String delimeter);

    public abstract Builder setFilePattern(String filePattern);

    public abstract Builder setInterval(Duration interval);

    public abstract Builder setKeyRange(Integer keyRange);

    public abstract CSVReaderTransform build();
  }

  public static Builder newBuilder() {
    return new AutoValue_CSVReaderTransform.Builder();
  }

  @Override
  public PCollectionTuple expand(PBegin input) {

    PCollection<KV<String, ReadableFile>> csvFile =
        input
            .apply(
                "Poll Input Files",
                FileIO.match()
                    .filepattern(filePattern())
                    .continuously(interval(), Watch.Growth.never()))
            .apply("Find Pattern Match", FileIO.readMatches().withCompression(Compression.AUTO))
            .apply("ValidateFile", ParDo.of(new FileSourceDoFn()));

    PCollection<KV<String, String>> contents =
        csvFile.apply("Read File", ParDo.of(new FileReaderSplitDoFn(delimeter(), keyRange())));

    return PCollectionTuple.of(Util.contentTag, contents).and(Util.headerTag, csvFile);
  }
}
