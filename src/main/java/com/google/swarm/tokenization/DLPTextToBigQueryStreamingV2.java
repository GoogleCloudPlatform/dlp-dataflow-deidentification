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
import com.google.swarm.tokenization.common.FilterBadRecordsDoFn;
import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.FileIO.ReadableFile;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DLPTextToBigQueryStreamingV2 {

  public static final Logger LOG = LoggerFactory.getLogger(DLPTextToBigQueryStreamingV2.class);
  private static final Duration DEFAULT_POLL_INTERVAL = Duration.standardSeconds(1);
  private static final Duration WINDOW_INTERVAL = Duration.standardSeconds(10);

  public static void main(String[] args) {

    DLPTextToBigQueryStreamingV2PipelineOptions options =
        PipelineOptionsFactory.fromArgs(args).as(DLPTextToBigQueryStreamingV2PipelineOptions.class);
    run(options);
  }

  public static PipelineResult run(DLPTextToBigQueryStreamingV2PipelineOptions defaultOptions) {

    TupleTag<KV<String, String>> goodRecords = new TupleTag<KV<String, String>>() {};
    TupleTag<KV<String, String>> badRecords = new TupleTag<KV<String, String>>() {};

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

    PCollectionTuple inputContents =
        inputFile
            .apply(
                "ReadFile",
                ParDo.of(
                    new FileReaderSplitDoFn(
                        defaultOptions.getKeyRange(), defaultOptions.getDelimeter())))
            .apply(
                "FilterBadRecords",
                ParDo.of(new FilterBadRecordsDoFn(goodRecords, badRecords, header))
                    .withSideInputs(header)
                    .withOutputTags(goodRecords, TupleTagList.of(badRecords)));

    PCollection<KV<String, TableRow>> transformedContents =
        inputContents
            .get(goodRecords)
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
                    .setJobName(defaultOptions.getJobName())
                    .build());

    transformedContents.apply(
        "StreamInsertToBQ",
        BigQueryDynamicWriteTransform.newBuilder()
            .setDatasetId(defaultOptions.getDataset())
            .setProjectId(defaultOptions.getProject())
            .build());

    inputContents
        .get(badRecords)
        .apply(
            "ExtractBadRecords",
            MapElements.into(TypeDescriptors.strings()).via(element -> element.getValue()))
        .apply(
            "LogBadRecords",
            TextIO.write()
                .to(defaultOptions.getBadRecordsLocation() + "/badrecords")
                .withWindowedWrites()
                .withNumShards(20));

    return p.run();
  }
}
