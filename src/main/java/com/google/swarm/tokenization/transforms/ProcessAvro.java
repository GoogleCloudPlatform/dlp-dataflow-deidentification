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
import com.google.swarm.tokenization.avro.AvroColumnNamesDoFn;
import com.google.swarm.tokenization.avro.AvroReaderSplittableDoFn;
import com.google.swarm.tokenization.avro.ConvertAvroRecordToDlpRowDoFn;
import com.google.swarm.tokenization.avro.GenericRecordCoder;
import com.google.swarm.tokenization.common.ResolveDuplicatesCombineFn;
import java.util.List;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;

@AutoValue
public abstract class ProcessAvro
    extends PTransform<PCollection<KV<String, FileIO.ReadableFile>>, PCollectionTuple> {

  public static final TupleTag<KV<String, Table.Row>> recordsTuple =
      new TupleTag<KV<String, Table.Row>>() {};
  public static final TupleTag<KV<String, List<String>>> headersTuple =
      new TupleTag<KV<String, List<String>>>() {};

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract ProcessAvro build();
  }

  public static ProcessAvro.Builder newBuilder() {
    return new AutoValue_ProcessAvro.Builder();
  }

  @Override
  public PCollectionTuple expand(PCollection<KV<String, FileIO.ReadableFile>> input) {

    PCollection<KV<String, List<String>>> headers =
        input
            .apply("ReadHeader", ParDo.of(new AvroColumnNamesDoFn()))
            .apply(
                Combine.<String, List<String>, List<String>>perKey(
                    new ResolveDuplicatesCombineFn()));

    PCollection<KV<String, Table.Row>> records =
        input
            .apply(ParDo.of(new AvroReaderSplittableDoFn(100, 900 * 1000)))
            .setCoder(KvCoder.of(StringUtf8Coder.of(), GenericRecordCoder.of()))
            .apply(ParDo.of(new ConvertAvroRecordToDlpRowDoFn()));

    return PCollectionTuple.of(recordsTuple, records).and(headersTuple, headers);
  }
}
