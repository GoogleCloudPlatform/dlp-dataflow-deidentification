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
import com.google.privacy.dlp.v2.Table;
import com.google.swarm.tokenization.common.ResolveDuplicatesCombineFn;
import com.google.swarm.tokenization.options.InspectClassifyPipelineOptions;
import com.google.swarm.tokenization.parquet.ParquetColumnNamesDoFn;
import com.google.swarm.tokenization.parquet.ParquetReaderSplittableDoFn;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.*;

@AutoValue
public abstract class ProcessParquet
    extends PTransform<PCollection<KV<String, FileIO.ReadableFile>>, PCollectionTuple> {

  public static final TupleTag<KV<String, Table.Row>> recordsTuple =
      new TupleTag<KV<String, Table.Row>>() {};
  public static final TupleTag<KV<String, List<String>>> headersTuple =
      new TupleTag<KV<String, List<String>>>() {};

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract ProcessParquet build();
  }

  public static ProcessParquet.Builder newBuilder() {
    return new AutoValue_ProcessParquet.Builder();
  }

  @Override
  public PCollectionTuple expand(PCollection<KV<String, FileIO.ReadableFile>> input) {

    InspectClassifyPipelineOptions options =
        input.getPipeline().getOptions().as(InspectClassifyPipelineOptions.class);

    PCollection<KV<String, List<String>>> headers =
        input
            .apply("ReadHeader", ParDo.of(new ParquetColumnNamesDoFn()))
            .apply(
                Combine.<String, List<String>, List<String>>perKey(
                    new ResolveDuplicatesCombineFn()));

    PCollectionView<Map<String, List<String>>> headersMap =
        headers.apply("ViewAsList", View.asMap());

    PCollection<KV<String, Table.Row>> records =
        input.apply(
            ParDo.of(
                new ParquetReaderSplittableDoFn(options.getKeyRange(), options.getSplitSize())));

    return PCollectionTuple.of(recordsTuple, records).and(headersTuple, headers);
  }
}
