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
import com.google.swarm.tokenization.avro.AvroReaderSplittableDoFn;
import com.google.swarm.tokenization.avro.ConvertAvroRecordToDlpRowDoFn;
import com.google.swarm.tokenization.avro.GenericRecordCoder;
import com.google.swarm.tokenization.beam.ConvertCSVRecordToDLPRow;
import com.google.swarm.tokenization.common.BigQueryDynamicWriteTransform;
import com.google.swarm.tokenization.common.BigQueryReadTransform;
import com.google.swarm.tokenization.common.BigQueryTableHeaderDoFn;
import com.google.swarm.tokenization.common.CSVFileReaderSplitDoFn;
import com.google.swarm.tokenization.common.DLPTransform;
import com.google.swarm.tokenization.common.ExtractColumnNamesTransform;
import com.google.swarm.tokenization.common.FilePollingTransform;
import com.google.swarm.tokenization.common.MergeBigQueryRowToDlpRow;
import com.google.swarm.tokenization.common.PubSubMessageConverts;
import com.google.swarm.tokenization.common.Util;
import com.google.swarm.tokenization.json.ConvertJsonRecordToDLPRow;
import com.google.swarm.tokenization.json.JsonReaderSplitDoFn;
import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.FileIO.ReadableFile;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.GroupByKey;
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
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DLPTextToBigQueryStreamingV2 {

  public static final Logger LOG = LoggerFactory.getLogger(DLPTextToBigQueryStreamingV2.class);
  private static final Duration DEFAULT_POLL_INTERVAL = Duration.standardSeconds(3);
  private static final Duration WINDOW_INTERVAL = Duration.standardSeconds(3);
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
            p.apply(
                FilePollingTransform.newBuilder()
                    .setFilePattern(options.getFilePattern())
                    .setInterval(DEFAULT_POLL_INTERVAL)
                    .build());
        final PCollectionView<List<String>> header =
            inputFiles
                .apply(
                    "GlobalWindow",
                    Window.<KV<String, FileIO.ReadableFile>>into(new GlobalWindows())
                        .triggering(
                            Repeatedly.forever(AfterProcessingTime.pastFirstElementInPane()))
                        .discardingFiredPanes())
                .apply(
                    ExtractColumnNamesTransform.newBuilder()
                        .setFileType(options.getFileType())
                        .setHeaders(options.getHeaders())
                        .build());
        PCollection<KV<String, Table.Row>> records;

        switch (options.getFileType()) {
          case AVRO:
            records =
                inputFiles
                    .apply(
                        ParDo.of(
                            new AvroReaderSplittableDoFn(
                                options.getKeyRange(), options.getSplitSize())))
                    .setCoder(KvCoder.of(StringUtf8Coder.of(), GenericRecordCoder.of()))
                    .apply(ParDo.of(new ConvertAvroRecordToDlpRowDoFn()));
            break;
          case CSV:
            records =
                inputFiles
                    .apply(
                        "SplitCSVFile",
                        ParDo.of(
                            new CSVFileReaderSplitDoFn(
                                options.getKeyRange(),
                                options.getRecordDelimiter(),
                                options.getSplitSize())))
                    .apply(
                        "ConvertToDLPRow",
                        ParDo.of(new ConvertCSVRecordToDLPRow(options.getColumnDelimiter(), header))
                            .withSideInputs(header));
            break;
          case JSON:
            records =
                inputFiles
                    .apply(
                        "SplitJSONFile",
                        ParDo.of(
                            new JsonReaderSplitDoFn(
                                options.getKeyRange(),
                                options.getRecordDelimiter(),
                                options.getSplitSize())))
                    .apply("ConvertToDLPRow", ParDo.of(new ConvertJsonRecordToDLPRow()));
            break;
          default:
            throw new IllegalArgumentException("Please validate FileType parameter");
        }

        records
            .apply("Fixed Window", Window.into(FixedWindows.of(WINDOW_INTERVAL)))
            .apply(
                "DLPTransform",
                DLPTransform.newBuilder()
                    .setBatchSize(options.getBatchSize())
                    .setInspectTemplateName(options.getInspectTemplateName())
                    .setDeidTemplateName(options.getDeidentifyTemplateName())
                    .setDlpmethod(options.getDLPMethod())
                    .setProjectId(options.getProject())
                    .setHeader(header)
                    .setColumnDelimiter(options.getColumnDelimiter())
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

        PCollection<KV<String, TableRow>> reidData =
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
                        .setColumnDelimiter(options.getColumnDelimiter())
                        .setJobName(options.getJobName())
                        .build())
                .get(Util.reidSuccess);

        // BQ insert
        reidData.apply(
            "BigQueryInsert",
            BigQueryDynamicWriteTransform.newBuilder()
                .setDatasetId(options.getDataset())
                .setProjectId(options.getProject())
                .build());
        // pubsub publish
        if (options.getTopic() != null) {
          reidData
              .apply("ConvertToPubSubMessage", ParDo.of(new PubSubMessageConverts()))
              .apply(
                  "PublishToPubSub",
                  PubsubIO.writeMessages()
                      .withMaxBatchBytesSize(PUB_SUB_BATCH_SIZE_BYTES)
                      .withMaxBatchSize(PUB_SUB_BATCH_SIZE)
                      .to(options.getTopic()));
        }

        return p.run();
      default:
        throw new IllegalArgumentException("Please validate DLPMethod param!");
    }
  }
}
