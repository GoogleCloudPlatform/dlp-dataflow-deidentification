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

import com.google.swarm.tokenization.common.BQWriteTransform;
import com.google.swarm.tokenization.common.DLPTransform;
import com.google.swarm.tokenization.common.S3ReaderOptions;
import com.google.swarm.tokenization.common.S3ReaderTransform;
import com.google.swarm.tokenization.common.Util;
import java.io.BufferedReader;
import java.io.IOException;
import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.FileIO.ReadableFile;
import org.apache.beam.sdk.io.ReadableFileCoder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.Row;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DLPS3ScannerPipeline {
  public static final Logger LOG = LoggerFactory.getLogger(DLPS3ScannerPipeline.class);
  private static final Duration DEFAULT_POLL_INTERVAL = Duration.standardSeconds(10);
  private static final Duration WINDOW_INTERVAL = Duration.standardSeconds(5);

  public static void main(String[] args) {
    S3ReaderOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(S3ReaderOptions.class);
    run(options);
  }

  public static PipelineResult run(S3ReaderOptions options) {
    Pipeline p = Pipeline.create(options);

    PCollection<KV<String, Iterable<ReadableFile>>> csvFile =
        p.apply("Poll Input Files", FileIO.match().filepattern(options.getCSVFilePattern()))
            .apply("Find Pattern Match", FileIO.readMatches().withCompression(Compression.AUTO))
            .apply("Add File Name as Key", WithKeys.of(file -> Util.getFileName(file)))
            .setCoder(KvCoder.of(StringUtf8Coder.of(), ReadableFileCoder.of()))
            .apply(GroupByKey.create());
    PCollectionView<List<String>> header =
        csvFile
            .apply(
                "AddHeaderAsSideInput",
                ParDo.of(
                    new DoFn<KV<String, Iterable<ReadableFile>>, String>() {

                      @ProcessElement
                      public void processElement(ProcessContext c) {
                        String fileName = c.element().getKey();
                        ReadableFile file = c.element().getValue().iterator().next();
                        try (BufferedReader br = Util.getReader(file)) {
                          CSVRecord csvHeader = CSVFormat.DEFAULT.parse(br).getRecords().get(0);
                          csvHeader.forEach(
                              headerValue -> {
                                c.output(headerValue);
                              });
                        } catch (IOException e) {
                          LOG.error("Failed to get csv header values}", e.getMessage());
                          throw new RuntimeException(e);
                        }
                      }
                    }))
            .apply("View As List", View.asList());

    PCollection<KV<String, String>> nonInspectedContents =
        csvFile.apply(
            "CSVReaderTransform", S3ReaderTransform.newBuilder().setDelimeter("\n").build());

    PCollection<Row> inspectedContents =
        nonInspectedContents
            .apply(
                "S3DLPScanner",
                DLPTransform.newBuilder()
                    .setBatchSize(options.getBatchSize())
                    .setInspectTemplateName(options.getInspectTemplateName())
                    .setProjectId(options.getProject())
                    .setCsvHeader(header)
                    .build())
            .setRowSchema(Util.dlpInspectionSchema);

    inspectedContents.apply(
        "StreamInsertToBQ",
        BQWriteTransform.newBuilder()
            .setTableSpec(options.getTableSpec())
            .setMethod(options.getWriteMethod())
            .build());
    return p.run();
  }
}
