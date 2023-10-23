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
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.FileIO.ReadableFile;
import org.apache.beam.sdk.io.ReadableFileCoder;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;

/** Transform that polls for new files and sanitizes file names. */
@AutoValue
public abstract class ReadExistingFilesTransform
    extends PTransform<PBegin, PCollection<KV<String, ReadableFile>>> {

  public abstract String filePattern();

  public abstract Boolean processExistingFiles();

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract ReadExistingFilesTransform.Builder setFilePattern(String filePattern);

    public abstract ReadExistingFilesTransform.Builder setProcessExistingFiles(
        Boolean processExistingFiles);

    public abstract ReadExistingFilesTransform build();
  }

  public static ReadExistingFilesTransform.Builder newBuilder() {
    return new AutoValue_ReadExistingFilesTransform.Builder();
  }

  @Override
  public PCollection<KV<String, ReadableFile>> expand(PBegin input) {
    if (!processExistingFiles()) {
      return input.apply(
          "CreateEmptyExistingFileSet",
          Create.empty(KvCoder.of(NullableCoder.of(StringUtf8Coder.of()), ReadableFileCoder.of())));
    }
    return input
        .apply("MatchExistingFiles", FileIO.match().filepattern(filePattern()))
        .apply("ReadExistingFiles", FileIO.readMatches())
        .apply(
            "SanitizeFileNameExistingFiles", ParDo.of(new SanitizeFileNameDoFn(InputLocation.GCS)));
  }
}
