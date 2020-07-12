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
import com.google.swarm.tokenization.common.BigQueryDynamicWriteTransform;
import com.google.swarm.tokenization.common.CSVFileHeaderDoFn;
import com.google.swarm.tokenization.common.CSVReaderTransform;
import com.google.swarm.tokenization.common.DLPTransform;
import com.google.swarm.tokenization.common.FileReaderSplitDoFn;
import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.FileIO.ReadableFile;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.commons.csv.CSVRecord;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DLPTextToBigQueryStreamingV2 {
  public static final Logger LOG = LoggerFactory.getLogger(DLPTextToBigQueryStreamingV2.class);
  private static final Duration DEFAULT_POLL_INTERVAL = Duration.standardSeconds(10);
  private static final Duration WINDOW_INTERVAL = Duration.standardSeconds(10);

  public static void main(String[] args) {

    DLPTextToBigQueryStreamingV2PipelineOptions defaultoptions =
        PipelineOptionsFactory.fromArgs(args).as(DLPTextToBigQueryStreamingV2PipelineOptions.class);

    run(defaultoptions);
  }

  public static PipelineResult run(DLPTextToBigQueryStreamingV2PipelineOptions defaultOptions) {

    Pipeline p = Pipeline.create(defaultOptions);

    PCollection<KV<String, ReadableFile>> inputFile =
        p.apply(
            "CSVReaderTransform",
            CSVReaderTransform.newBuilder()
                .setDelimeter(defaultOptions.getDelimeter())
                .setFilePattern(defaultOptions.getCSVFilePattern())
                .setKeyRange(defaultOptions.getKeyRange())
                .setInterval(DEFAULT_POLL_INTERVAL)
                .build());

    final PCollectionView<List<String>> header =
        inputFile
            .apply(
                "GlobalWindow",
                Window.<KV<String, ReadableFile>>into(new GlobalWindows())
                    .triggering(Repeatedly.forever(AfterProcessingTime.pastFirstElementInPane()))
                    .discardingFiredPanes())
            .apply("ReadHeader", ParDo.of(new CSVFileHeaderDoFn()))
            .apply("ViewAsList", View.asList());

    PCollection<KV<String, TableRow>> inspectedContents =
        inputFile
            .apply("ReadFile", ParDo.of(new FileReaderSplitDoFn(defaultOptions.getKeyRange())))
            .apply(
                "Fixed Window",
                Window.<KV<String, CSVRecord>>into(FixedWindows.of(WINDOW_INTERVAL)))
            .apply(
                "DLPTransform",
                DLPTransform.newBuilder()
                    .setBatchSize(defaultOptions.getBatchSize())
                    .setInspectTemplateName(defaultOptions.getInspectTemplateName())
                    .setDeidTemplateName(defaultOptions.getDeidentifyTemplateName())
                    .setDlpmethod(defaultOptions.getDLPMethod())
                    .setProjectId(defaultOptions.getProject())
                    .setCsvHeader(header)
                    .setColumnDelimeter(defaultOptions.getColumnDelimeter())
                    .build());

    inspectedContents.apply(
        "StreamInsertToBQ",
        BigQueryDynamicWriteTransform.newBuilder()
            .setDatasetId(defaultOptions.getDataset())
            .setProjectId(defaultOptions.getProject())
            .build());
    return p.run();
  }
}
