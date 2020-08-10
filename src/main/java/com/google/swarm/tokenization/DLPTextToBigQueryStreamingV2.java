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
import com.google.privacy.dlp.v2.Table;
import com.google.protobuf.ByteString;
import com.google.swarm.tokenization.avro.*;
import com.google.swarm.tokenization.beam.MapStringToDlpRow;
import com.google.swarm.tokenization.common.*;
import java.util.List;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.FileIO.ReadableFile;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.windowing.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DLPTextToBigQueryStreamingV2 {

  public static final Logger LOG = LoggerFactory.getLogger(DLPTextToBigQueryStreamingV2.class);
  private static final Duration DEFAULT_POLL_INTERVAL = Duration.standardSeconds(10);
  private static final Duration WINDOW_INTERVAL = Duration.standardSeconds(10);
  /** PubSub configuration for default batch size in number of messages */
  public static final Integer PUB_SUB_BATCH_SIZE = 1000;
  /** PubSub configuration for default batch size in bytes */
  public static final Integer PUB_SUB_BATCH_SIZE_BYTES = 10000;

  public static void main(String[] args) {

    DLPTextToBigQueryStreamingV2PipelineOptions options =
        PipelineOptionsFactory.fromArgs(args).as(DLPTextToBigQueryStreamingV2PipelineOptions.class);
    run(options);
  }

  public static PipelineResult run(DLPTextToBigQueryStreamingV2PipelineOptions options) {
    Pipeline p = Pipeline.create(options);

    switch (options.getDLPMethod()) {
      case INSPECT:
      case DEID:
        PCollection<KV<String, ReadableFile>> inputFiles =
            p
                .apply(
                    FilePollingTransform
                        .newBuilder()
                        .setFilePattern(options.getFilePattern())
                        .setInterval(DEFAULT_POLL_INTERVAL)
                        .build()
                );
        final PCollectionView<List<String>> header =
            inputFiles
              .apply(
                  ExtractColumnNamesTransform
                      .newBuilder()
                      .setFileType(options.getFileType())
                      .build()
              );
        PCollection<KV<String, Table.Row>> records;

        switch (options.getFileType()) {
          case AVRO:
            PCollectionView<ByteString> binaryHeader =
                inputFiles
                .apply(
                    AvroBinaryHeaderTransform
                    .newBuilder()
                    .build()
                );
            records = inputFiles
                .apply(
                    "SplitAvroFile",
                    ParDo.of(new AvroReaderSplitDoFn(options.getKeyRange(), options.getSplitSize())))
                .apply(
                    ParDo.of(new ReadAvroBlocks(options.getKeyRange(), binaryHeader))
                        .withSideInputs(binaryHeader)
                );
            break;
          case CSV:
            records = inputFiles
                .apply(
                    "SplitCSVFile",
                    ParDo.of(new CSVFileReaderSplitDoFn(options.getKeyRange(), options.getDelimeter(), options.getSplitSize())))
                .apply(ParDo.of(new MapStringToDlpRow(options.getColumnDelimeter())));
            break;
          default:
            throw new IllegalArgumentException("Please validate FileType parameter");
        }

        records
            .apply(
                "Fixed Window",
                Window.into(FixedWindows.of(WINDOW_INTERVAL)))
            .apply(
                "DLPTransform",
                DLPTransform.newBuilder()
                    .setBatchSize(options.getBatchSize())
                    .setInspectTemplateName(options.getInspectTemplateName())
                    .setDeidTemplateName(options.getDeidentifyTemplateName())
                    .setDlpmethod(options.getDLPMethod())
                    .setProjectId(options.getProject())
                    .setHeader(header)
                    .setColumnDelimeter(options.getColumnDelimeter())
                    .setJobName(options.getJobName())
                    .build())
            .get(Util.inspectOrDeidSuccess)
            .apply(
                "StreamInsertToBQ",
                BigQueryDynamicWriteTransform.newBuilder()
                    .setDatasetId(options.getDataset())
                    .setProjectId(options.getProject())
                    .build());

        return p.run();

      case REID:
        PCollection<KV<String, TableRow>> record =
            p.apply(
                "ReadFromBQ",
                BigQueryReadTransform.newBuilder()
                    .setTableRef(options.getTableRef())
                    .setReadMethod(options.getReadMethod())
                    .setKeyRange(options.getKeyRange())
                    .setQuery(Util.getQueryFromGcs(options.getQueryPath()))
                    .build());

        PCollectionView<List<String>> selectedColumns =
            record
                .apply(
                    "GlobalWindow",
                    Window.<KV<String, TableRow>>into(new GlobalWindows())
                        .triggering(
                            Repeatedly.forever(AfterProcessingTime.pastFirstElementInPane()))
                        .discardingFiredPanes())
                .apply("GroupByTableName", GroupByKey.create())
                .apply("GetHeader", ParDo.of(new BigQueryTableHeaderDoFn()))
                .apply("ViewAsList", View.asList());

        record
            .apply("ConvertTableRow", ParDo.of(new MergeBigQueryRowToDlpRow()))
            .apply(
                "DLPTransform",
                DLPTransform.newBuilder()
                    .setBatchSize(options.getBatchSize())
                    .setInspectTemplateName(options.getInspectTemplateName())
                    .setDeidTemplateName(options.getDeidentifyTemplateName())
                    .setDlpmethod(options.getDLPMethod())
                    .setProjectId(options.getProject())
                    .setHeader(selectedColumns)
                    .setColumnDelimeter(",")
                    .setJobName(options.getJobName())
                    .build())
            .get(Util.reidSuccess)
            .apply(
                "PublishToPubSub",
                PubsubIO.writeMessages()
                    .withMaxBatchBytesSize(PUB_SUB_BATCH_SIZE_BYTES)
                    .withMaxBatchSize(PUB_SUB_BATCH_SIZE)
                    .to(options.getTopic()));
        return p.run();
      default:
        throw new IllegalArgumentException("Please validate DLPMethod param!");
    }
  }
}
