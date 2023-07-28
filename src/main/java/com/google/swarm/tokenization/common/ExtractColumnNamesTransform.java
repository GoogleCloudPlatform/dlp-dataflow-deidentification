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
import com.google.swarm.tokenization.avro.AvroColumnNamesDoFn;
import com.google.swarm.tokenization.common.Util.FileType;
import com.google.swarm.tokenization.json.JsonColumnNameDoFn;
import com.google.swarm.tokenization.txt.TxtColumnNameDoFn;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;

@SuppressWarnings("serial")
@AutoValue
public abstract class ExtractColumnNamesTransform
    extends PTransform<
        PCollection<KV<String, FileIO.ReadableFile>>, PCollectionView<Map<String, List<String>>>> {

  public abstract FileType fileType();

  @Nullable
  public abstract List<String> headers();

  public abstract Character columnDelimiter();

  public abstract Boolean pubSubGcs();

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract ExtractColumnNamesTransform.Builder setFileType(FileType fileType);

    public abstract ExtractColumnNamesTransform.Builder setHeaders(List<String> headers);

    public abstract ExtractColumnNamesTransform.Builder setColumnDelimiter(
        Character columnDelimiter);

    public abstract ExtractColumnNamesTransform.Builder setPubSubGcs(
        Boolean pubSubGcs);

    public abstract ExtractColumnNamesTransform build();
  }

  public static ExtractColumnNamesTransform.Builder newBuilder() {
    return new AutoValue_ExtractColumnNamesTransform.Builder();
  }

  @Override
  public PCollectionView<Map<String, List<String>>> expand(
      PCollection<KV<String, FileIO.ReadableFile>> input) {
    PCollection<KV<String, List<String>>> readHeader;
    switch (fileType()) {
      case CSV:
        readHeader = input.apply("ReadHeader", ParDo.of(new CSVColumnNamesDoFn(columnDelimiter())));
        break;

      case TSV:
        readHeader = input.apply("ReadHeader", ParDo.of(new CSVColumnNamesDoFn('\t')));
        break;

      case AVRO:
        readHeader = input.apply("ReadHeader", ParDo.of(new AvroColumnNamesDoFn()));
        break;

      case JSONL:
        readHeader = input.apply("ReadHeader", ParDo.of(new JsonColumnNameDoFn(headers())));
        break;

      case TXT:
        readHeader = input.apply("ReadHeader", ParDo.of(new TxtColumnNameDoFn(headers())));
        break;

      default:
        throw new IllegalStateException("Unexpected value: " + fileType());
    }
    if (!pubSubGcs()) {
        return readHeader.apply("ViewAsList", View.asMap());
    }
    return readHeader.apply(Combine.<String, List<String>, List<String>>perKey(new ResolveDuplicatesCombineFn()))
                     .apply("ViewAsList", View.asMap());
  }
}
