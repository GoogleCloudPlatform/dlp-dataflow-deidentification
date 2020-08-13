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
package com.google.swarm.tokenization.avro;

import java.io.IOException;

import com.google.auto.value.AutoValue;
import com.google.protobuf.ByteString;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;


/**
 * Returns the header (in byte form) for the given Avro files. To be used as side input.
 */
@AutoValue
public abstract class AvroBinaryHeaderTransform extends PTransform<PCollection<KV<String, FileIO.ReadableFile>>, PCollectionView<ByteString>> {

    @AutoValue.Builder
    public abstract static class Builder {
        public abstract AvroBinaryHeaderTransform build();
    }

    public static AvroBinaryHeaderTransform.Builder newBuilder() {
        return new AutoValue_AvroBinaryHeaderTransform.Builder();
    }

    public class AvroBinaryHeaderDoFn extends DoFn<KV<String, FileIO.ReadableFile>, ByteString> {

        @ProcessElement
        public void processElement(ProcessContext c) throws IOException {
            FileIO.ReadableFile file = c.element().getValue();
            ByteString header = AvroUtil.extractHeader(file);
            c.output(header);
        }
    }

    @Override
    public PCollectionView<ByteString> expand(PCollection<KV<String, FileIO.ReadableFile>> input) {
        return input
            .apply(ParDo.of(new AvroBinaryHeaderDoFn()))
            .apply(View.asSingleton());
    }

}
