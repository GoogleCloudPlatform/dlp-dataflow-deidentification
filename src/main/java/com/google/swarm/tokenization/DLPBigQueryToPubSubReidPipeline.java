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
package com.google.swarm.tokenization;

import com.google.api.services.bigquery.model.TableRow;
import com.google.swarm.tokenization.common.BigQueryReadTransform;
import com.google.swarm.tokenization.common.DLPTransform;
import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.joda.time.Duration;

public class DLPBigQueryToPubSubReidPipeline {
  private static final Duration WINDOW_INTERVAL = Duration.standardSeconds(10);

  public static void main(String args[]) {
    DLPBigQueryToPubSubReidPipelineOptions options =
        PipelineOptionsFactory.fromArgs(args).as(DLPBigQueryToPubSubReidPipelineOptions.class);
    run(options);
  }

  private static PipelineResult run(DLPBigQueryToPubSubReidPipelineOptions options) {
    Pipeline p = Pipeline.create(options);

    // side input
    final PCollectionView<List<String>> selectedColumns =
        p.apply(Create.of(options.getFields())).apply("ViewAsList", View.asList());

    PCollection<KV<String, TableRow>> recordWithKey =
        p.apply(
                "ReadFromBQ",
                BigQueryReadTransform.newBuilder()
                    .setTableRef(options.getTableRef())
                    .setReadMethod(options.getReadMethod())
                    .setFields(options.getFields())
                    .build())
            .apply(
                "Fixed Window", Window.<KV<String, String>>into(FixedWindows.of(WINDOW_INTERVAL)))
            .apply(
                "DLPTransform",
                DLPTransform.newBuilder()
                    .setBatchSize(options.getBatchSize())
                    .setInspectTemplateName(options.getInspectTemplateName())
                    .setDeidTemplateName(options.getDeidentifyTemplateName())
                    .setDlpmethod(options.getDLPMethod())
                    .setProjectId(options.getProject())
                    .setHeader(selectedColumns)
                    .setColumnDelimeter(options.getColumnDelimeter())
                    .build());
    return p.run();
  }
}
