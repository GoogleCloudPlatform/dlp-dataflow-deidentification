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
package com.google.swarm.tokenization.orc;

import com.google.auto.value.AutoValue;
import com.google.swarm.tokenization.common.Util;
import java.io.IOException;
import java.util.Map;
import org.apache.beam.sdk.io.FileIO.ReadableFile;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.orc.Reader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@AutoValue
@SuppressWarnings("serial")
public abstract class ExtractFileSchemaTransform
    extends PTransform<
        PCollection<KV<String, ReadableFile>>, PCollectionView<Map<String, String>>> {

  public static final Logger LOG = LoggerFactory.getLogger(ExtractFileSchemaTransform.class);

  public abstract Util.FileType fileType();

  public abstract String projectId();

  public static ExtractFileSchemaTransform.Builder newBuilder() {
    return new AutoValue_ExtractFileSchemaTransform.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setFileType(Util.FileType fileType);

    public abstract Builder setProjectId(String projectId);

    public abstract ExtractFileSchemaTransform build();
  }

  @Override
  public PCollectionView<Map<String, String>> expand(PCollection<KV<String, ReadableFile>> input) {
    PCollectionView<Map<String, String>> schemaMapping;

    schemaMapping =
        input
            .apply(
                ParDo.of(
                    new DoFn<KV<String, ReadableFile>, KV<String, String>>() {
                      @ProcessElement
                      public void processElement(ProcessContext c) throws IOException {
                        String filename = c.element().getKey();
                        ReadableFile randomFile = c.element().getValue();
                        String filePath = randomFile.getMetadata().resourceId().toString();
                        Reader reader =
                            new ORCFileReader().createORCFileReader(filePath, projectId());
                        String schema = reader.getSchema().toString();
                        c.output(KV.of(filename, schema));
                      }
                    }))
            .apply("ViewAsList", View.asMap());

    return schemaMapping;
  }
}
