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
package com.google.swarm.tokenization.pipelines;

import com.google.api.services.bigquery.model.TableRow;
import com.google.swarm.tokenization.beam.DLPInspectText;
import com.google.swarm.tokenization.classification.ClassifyFiles;
import com.google.swarm.tokenization.coders.DeterministicTableRowJsonCoder;
import com.google.swarm.tokenization.common.Util;
import com.google.swarm.tokenization.options.InspectClassifyPipelineOptions;
import com.google.swarm.tokenization.transforms.ProcessFiles;
import com.google.swarm.tokenization.transforms.ReadFilesWithGivenExtensions;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PCollectionView;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InspectClassify {

  public static final Logger LOG = LoggerFactory.getLogger(InspectClassify.class);
  private static final Duration DEFAULT_POLL_INTERVAL = Duration.standardSeconds(3);
  private static final Duration WINDOW_INTERVAL = Duration.standardSeconds(3);

  public static void main(String[] args) throws CannotProvideCoderException {

    InspectClassifyPipelineOptions options =
        PipelineOptionsFactory.fromArgs(args).as(InspectClassifyPipelineOptions.class);
    run(options);
  }

  public static PipelineResult run(InspectClassifyPipelineOptions options)
      throws CannotProvideCoderException {
    Pipeline p = Pipeline.create(options);
    p.getCoderRegistry().registerCoderForClass(TableRow.class, DeterministicTableRowJsonCoder.of());

    List<Util.FileType> fileExtensionList =
        options.getFileTypes().stream()
            .map(x -> Util.FileType.valueOf(x.toUpperCase()))
            .collect(Collectors.toList());

    /** Fetch List of input files */
    PCollection<KV<String, FileIO.ReadableFile>> inputFiles =
        p.apply(
                "ReadExistingFiles",
                ReadFilesWithGivenExtensions.newBuilder()
                    .setFilePattern(options.getFilePattern())
                    .setFileTypes(options.getFileTypes())
                    .build())
            .apply("Fixed Window", Window.into(FixedWindows.of(WINDOW_INTERVAL)));

    /** Process Files */
    PCollectionTuple records =
        inputFiles.apply(
            "ProcessFiles", ProcessFiles.newBuilder().setFileTypes(fileExtensionList).build());

    final PCollectionView<Map<String, List<String>>> headers =
        records.get(ProcessFiles.headersMap).apply("ViewAsList", View.asMap());

    records
        .get(ProcessFiles.tableRows)
        .apply(
            "DLPInspection",
            DLPInspectText.newBuilder()
                .setBatchSizeBytes(options.getBatchSize())
                .setColumnDelimiter(options.getColumnDelimiterForCsvFiles())
                .setHeaderColumns(headers)
                .setInspectTemplateName(options.getInspectTemplateName())
                .setProjectId(options.getDLPParent())
                .setDlpApiRetryCount(options.getDlpApiRetryCount())
                .setInitialBackoff(options.getInitialBackoff())
                .build())
        .apply(
            "ClassifyFiles",
            ClassifyFiles.newBuilder().setOutputPubSubTopic(options.getTopic()).build());

    return p.run();
  }
}
