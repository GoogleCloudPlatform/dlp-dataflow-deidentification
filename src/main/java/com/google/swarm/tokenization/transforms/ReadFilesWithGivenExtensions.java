/*
 * Copyright 2024 Google LLC
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
package com.google.swarm.tokenization.transforms;

import com.google.auto.value.AutoValue;
import com.google.swarm.tokenization.common.SanitizeFileNameDoFn;
import com.google.swarm.tokenization.common.Util;
import java.util.List;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;

@AutoValue
public abstract class ReadFilesWithGivenExtensions
    extends PTransform<PBegin, PCollection<KV<String, FileIO.ReadableFile>>> {

  public abstract String filePattern();

  public abstract List<String> fileTypes();

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract ReadFilesWithGivenExtensions.Builder setFilePattern(String filePattern);

    public abstract ReadFilesWithGivenExtensions.Builder setFileTypes(List<String> value);

    public abstract ReadFilesWithGivenExtensions build();
  }

  public static ReadFilesWithGivenExtensions.Builder newBuilder() {
    return new AutoValue_ReadFilesWithGivenExtensions.Builder();
  }

  @Override
  public PCollection<KV<String, FileIO.ReadableFile>> expand(PBegin input) {
    return input
        .apply("MatchExistingFiles", FileIO.match().filepattern(filePattern()))
        .apply(
            "FilterFiles",
            Filter.by(
                new SerializableFunction<MatchResult.Metadata, Boolean>() {
                  @Override
                  public Boolean apply(MatchResult.Metadata input) {
                    String filename = input.resourceId().getFilename();
                    String fileExtension = filename.substring(filename.lastIndexOf(".") + 1);
                    return fileTypes().contains(fileExtension);
                  }
                }))
        .apply("ReadExistingFiles", FileIO.readMatches())
        .apply(
            "SanitizeFileNameExistingFiles",
            ParDo.of(new SanitizeFileNameDoFn(Util.InputLocation.GCS)));
  }
}
