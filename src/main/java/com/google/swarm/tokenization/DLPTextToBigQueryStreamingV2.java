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
import com.google.swarm.tokenization.coders.DeterministicTableRowJsonCoder;
import com.google.swarm.tokenization.common.BigQueryDynamicWriteTransform;
import com.google.swarm.tokenization.common.BigQueryReadTransform;
import com.google.swarm.tokenization.common.BigQueryTableHeaderDoFn;
import com.google.swarm.tokenization.common.CSVFileReaderSplitDoFn;
import com.google.swarm.tokenization.common.DLPTransform;
import com.google.swarm.tokenization.common.ExtractColumnNamesTransform;
import com.google.swarm.tokenization.common.FilePollingTransform;
import com.google.swarm.tokenization.common.MergeBigQueryRowToDlpRow;
import com.google.swarm.tokenization.common.PubSubMessageConverts;
import com.google.swarm.tokenization.common.ReadExistingFilesTransform;
import com.google.swarm.tokenization.common.ReadNewFilesPubSubTransform;
import com.google.swarm.tokenization.common.Util;
import com.google.swarm.tokenization.common.Util.InputLocation;
import com.google.swarm.tokenization.common.WriteToGCS;
import com.google.swarm.tokenization.json.ConvertJsonRecordToDLPRow;
import com.google.swarm.tokenization.json.JsonReaderSplitDoFn;
import com.google.swarm.tokenization.orc.ExtractFileSchemaTransform;
import com.google.swarm.tokenization.orc.ORCReaderDoFn;
import com.google.swarm.tokenization.orc.ORCWriterDoFn;
import com.google.swarm.tokenization.parquet.ParquetReaderSplittableDoFn;
import com.google.swarm.tokenization.txt.ConvertTxtToDLPRow;
import com.google.swarm.tokenization.txt.ParseTextLogDoFn;
import com.google.swarm.tokenization.txt.TxtReaderSplitDoFn;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.FileIO.ReadableFile;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTagList;
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
    Util.validateBQStorageApiOptionsStreaming(options);
    run(options);
  }

  public static PipelineResult run(DLPTextToBigQueryStreamingV2PipelineOptions options) {
    Pipeline p = Pipeline.create(options);
    p.getCoderRegistry().registerCoderForClass(TableRow.class, DeterministicTableRowJsonCoder.of());

    switch (options.getDLPMethod()) {
      case INSPECT:
      case DEID:
        runInspectAndDeidPipeline(p, options);
        break;

      case REID:
        runReidPipeline(p, options);
        break;

      default:
        throw new IllegalArgumentException("Please validate DLPMethod param!");
    }

    return p.run();
  }

  private static void runInspectAndDeidPipeline(
      Pipeline p, DLPTextToBigQueryStreamingV2PipelineOptions options) {
    PCollection<KV<String, ReadableFile>> inputFiles;
    boolean usePubSub = options.getGcsNotificationTopic() != null;

    if (options.getInputProviderType() == InputLocation.GCS
        && !usePubSub
        && !options.getProcessExistingFiles())
      throw new IllegalArgumentException(
          "Either --processExistingFiles should be set to true or --gcsNotificationTopic should be provided");

    if (options.getDataset() != null && options.getOutputBucket() != null)
      throw new IllegalArgumentException("Please provide either BQ Dataset or GCS output bucket");

    if (options.getInputProviderType() == InputLocation.GCS) {
      PCollection<KV<String, ReadableFile>> newFiles =
          p.apply(
              "Read New Files",
              ReadNewFilesPubSubTransform.newBuilder()
                  .setFilePattern(options.getFilePattern())
                  .setUsePubSub(usePubSub)
                  .setPubSubTopic(options.getGcsNotificationTopic())
                  .build());

      PCollection<KV<String, ReadableFile>> existingFiles =
          p.apply(
              "Read Existing Files",
              ReadExistingFilesTransform.newBuilder()
                  .setFilePattern(options.getFilePattern())
                  .setProcessExistingFiles(options.getProcessExistingFiles())
                  .build());

      inputFiles =
          PCollectionList.of(newFiles)
              .and(existingFiles)
              .apply(Flatten.<KV<String, ReadableFile>>pCollections());
    } else {
      inputFiles =
          p.apply(
              FilePollingTransform.newBuilder()
                  .setFilePattern(options.getFilePattern())
                  .setInterval(DEFAULT_POLL_INTERVAL)
                  .build());
    }

    inputFiles = inputFiles.apply("Fixed Window", Window.into(FixedWindows.of(WINDOW_INTERVAL)));

    final PCollectionView<Map<String, List<String>>> headers =
        inputFiles.apply(
            "Extract Column Names",
            ExtractColumnNamesTransform.newBuilder()
                .setFileType(options.getFileType())
                .setHeaders(options.getHeaders())
                .setColumnDelimiter(options.getColumnDelimiter())
                .setPubSubGcs(usePubSub)
                .setProjectId(options.getProject())
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
      case TSV:
        options.setColumnDelimiter('\t');
      case CSV:
        records =
            inputFiles
                .apply(
                    "SplitCSVFile",
                    ParDo.of(
                        new CSVFileReaderSplitDoFn(
                            options.getRecordDelimiter(), options.getSplitSize())))
                .apply(
                    "ConvertToDLPRow",
                    ParDo.of(new ConvertCSVRecordToDLPRow(options.getColumnDelimiter(), headers))
                        .withSideInputs(headers));
        break;
      case JSONL:
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
      case TXT:
        PCollectionTuple recordTuple =
            inputFiles
                .apply(
                    "SplitTextFile",
                    ParDo.of(
                        new TxtReaderSplitDoFn(
                            options.getKeyRange(),
                            options.getRecordDelimiter(),
                            options.getSplitSize())))
                .apply(
                    "ParseTextFile",
                    ParDo.of(new ParseTextLogDoFn())
                        .withOutputTags(
                            Util.agentTranscriptTuple,
                            TupleTagList.of(Util.customerTranscriptTuple)));

        records =
            PCollectionList.of(recordTuple.get(Util.agentTranscriptTuple))
                .and(recordTuple.get(Util.customerTranscriptTuple))
                .apply("Flatten", Flatten.pCollections())
                .apply(
                    "ConvertToDLPRow",
                    ParDo.of(new ConvertTxtToDLPRow(options.getColumnDelimiter(), headers))
                        .withSideInputs(headers));
        break;
      case PARQUET:
        //      TODO: Remove KeyRange parameter, as it is unused
        records =
            inputFiles.apply(
                ParDo.of(
                    new ParquetReaderSplittableDoFn(
                        options.getKeyRange(), options.getSplitSize())));
        break;

      case ORC:
        records =
            inputFiles.apply("ReadORCFiles", ParDo.of(new ORCReaderDoFn(options.getProject())));
        break;

      default:
        throw new IllegalArgumentException("Please validate FileType parameter");
    }

    PCollectionTuple inspectDeidRecords =
        records.apply(
            "DLPTransform",
            DLPTransform.newBuilder()
                .setBatchSize(options.getBatchSize())
                .setInspectTemplateName(options.getInspectTemplateName())
                .setDeidTemplateName(options.getDeidentifyTemplateName())
                .setDlpmethod(options.getDLPMethod())
                .setProjectId(options.getDLPParent())
                .setHeaders(headers)
                .setColumnDelimiter(options.getColumnDelimiter())
                .setJobName(options.getJobName())
                .setDlpApiRetryCount(options.getDlpApiRetryCount())
                .setInitialBackoff(options.getInitialBackoff())
                .setDataSinkType(options.getDataSinkType())
                .build());

    if (options.getDataSinkType() == Util.DataSinkType.GCS) {
      if (options.getFileType() == Util.FileType.ORC) {
        final PCollectionView<Map<String, String>> schemaMapping =
            inputFiles.apply(
                "Extract Input File Schema",
                ExtractFileSchemaTransform.newBuilder()
                    .setFileType(options.getFileType())
                    .setProjectId(options.getProject())
                    .build());

        inspectDeidRecords
            .get(Util.deidSuccessGCS)
            .apply(GroupByKey.create())
            .apply(
                "WriteORCToGCS",
                ParDo.of(new ORCWriterDoFn(options.getOutputBucket(), schemaMapping))
                    .withSideInputs(schemaMapping))
            .setCoder(StringUtf8Coder.of());
      } else {
        inspectDeidRecords
            .get(Util.deidSuccessGCS)
            .apply(
                "WriteToGCS",
                WriteToGCS.newBuilder()
                    .setOutputBucket(options.getOutputBucket())
                    .setFileType(options.getFileType())
                    .setColumnDelimiter(options.getColumnDelimiter())
                    .build());
      }
    } else if (options.getDataSinkType() == Util.DataSinkType.BigQuery) {
      inspectDeidRecords
          .get(Util.inspectOrDeidSuccess)
          .apply(
              "InsertToBQ",
              BigQueryDynamicWriteTransform.newBuilder()
                  .setDatasetId(options.getDataset())
                  .setProjectId(options.getProject())
                  .build());
    }
  }

  private static void runReidPipeline(
      Pipeline p, DLPTextToBigQueryStreamingV2PipelineOptions options) {
    // TODO: there is no reason for this method to key elements by table reference because
    // there is always a single possible reference for this batch pipeline.
    // Changing it will require additional refactoring which is outside of the scope of the current
    // fix.
    PCollection<KV<String, TableRow>> records =
        p.apply(
            "ReadFromBQ",
            BigQueryReadTransform.newBuilder()
                .setTableRef(options.getTableRef())
                .setReadMethod(options.getReadMethod())
                .setKeyRange(options.getKeyRange())
                .setQuery(Util.getQueryFromGcs(options.getQueryPath()))
                .build());

    PCollectionView<Map<String, List<String>>> selectedColumns =
        records
            .apply("GetARow", Sample.any(1))
            .apply("GetColumns", ParDo.of(new BigQueryTableHeaderDoFn()))
            .apply("CreateSideInput", View.asMap());

    PCollection<KV<String, TableRow>> reidData =
        records
            .apply("ConvertTableRow", ParDo.of(new MergeBigQueryRowToDlpRow()))
            .apply(
                "DLPTransform",
                DLPTransform.newBuilder()
                    .setBatchSize(options.getBatchSize())
                    .setInspectTemplateName(options.getInspectTemplateName())
                    .setDeidTemplateName(options.getDeidentifyTemplateName())
                    .setDlpmethod(options.getDLPMethod())
                    .setProjectId(options.getDLPParent())
                    .setHeaders(selectedColumns)
                    .setColumnDelimiter(options.getColumnDelimiter())
                    .setJobName(options.getJobName())
                    .setDlpApiRetryCount(options.getDlpApiRetryCount())
                    .setInitialBackoff(options.getInitialBackoff())
                    .setDataSinkType(options.getDataSinkType())
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
  }
}
