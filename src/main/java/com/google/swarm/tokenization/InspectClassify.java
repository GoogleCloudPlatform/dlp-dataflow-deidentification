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
package com.google.swarm.tokenization;

import com.google.api.services.bigquery.model.TableRow;
import com.google.privacy.dlp.v2.FileType;
import com.google.privacy.dlp.v2.Table;
import com.google.swarm.tokenization.avro.AvroColumnNamesDoFn;
import com.google.swarm.tokenization.avro.AvroReaderSplittableDoFn;
import com.google.swarm.tokenization.avro.ConvertAvroRecordToDlpRowDoFn;
import com.google.swarm.tokenization.avro.GenericRecordCoder;
import com.google.swarm.tokenization.beam.ConvertCSVRecordToDLPRow;
import com.google.swarm.tokenization.beam.DLPInspectText;
import com.google.swarm.tokenization.classification.ClassifyFiles;
import com.google.swarm.tokenization.classification.NewExtractColumnNamesTransform;
import com.google.swarm.tokenization.classification.ReadHeaderTransform;
import com.google.swarm.tokenization.coders.DeterministicTableRowJsonCoder;
import com.google.swarm.tokenization.common.*;
import com.google.swarm.tokenization.json.JsonColumnNameDoFn;
import com.google.swarm.tokenization.orc.ORCColumnNameDoFn;
import com.google.swarm.tokenization.parquet.ParquetColumnNamesDoFn;
import com.google.swarm.tokenization.txt.TxtColumnNameDoFn;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.*;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InspectClassify {

  public static final Logger LOG = LoggerFactory.getLogger(InspectClassify.class);
  private static final Duration DEFAULT_POLL_INTERVAL = Duration.standardSeconds(3);
  private static final Duration WINDOW_INTERVAL = Duration.standardSeconds(3);

  /** PubSub configuration for default batch size in number of messages */
  public static final Integer PUB_SUB_BATCH_SIZE = 1000;

  /** PubSub configuration for default batch size in bytes */
  public static final Integer PUB_SUB_BATCH_SIZE_BYTES = 10000;

  public static void main(String[] args) throws CannotProvideCoderException {

    InspectClassifyPipelineOptions options =
        PipelineOptionsFactory.fromArgs(args).as(InspectClassifyPipelineOptions.class);
    run(options);
  }

  public static PCollectionView<Map<String, List<String>>> createHeaders(
      PCollectionList<KV<String, FileIO.ReadableFile>> input,
      Pipeline p,
      List<Util.FileType> fileTypes,
      InspectClassifyPipelineOptions options) {

    PCollection<KV<String, List<String>>> readHeader;
    PCollectionList<KV<String, List<String>>> headerList = PCollectionList.empty(p);

    for (Util.FileType fileType : fileTypes) {
      switch (fileType) {
        case AVRO:
          PCollection<KV<String, List<String>>> readHeaderAvro =
              input
                  .get(fileType.ordinal())
                  .apply("ReadHeader", ParDo.of(new AvroColumnNamesDoFn()));
          headerList.and(readHeaderAvro);
          break;

        case CSV:
          PCollection<KV<String, List<String>>> readHeaderCsv =
              input
                  .get(fileType.ordinal())
                  .apply(
                      "ReadHeader", ParDo.of(new CSVColumnNamesDoFn(options.getColumnDelimiter())));
          headerList.and(readHeaderCsv);
          break;

        case JSONL:
          PCollection<KV<String, List<String>>> readHeaderJsonl =
              input
                  .get(fileType.ordinal())
                  .apply("ReadHeader", ParDo.of(new JsonColumnNameDoFn(options.getHeaders())));
          headerList.and(readHeaderJsonl);
          break;

        case ORC:
          PCollection<KV<String, List<String>>> readHeaderOrc =
              input
                  .get(fileType.ordinal())
                  .apply("ReadHeader", ParDo.of(new ORCColumnNameDoFn(options.getProject())));
          headerList.and(readHeaderOrc);
          break;

        case PARQUET:
          PCollection<KV<String, List<String>>> readHeaderParquet =
              input
                  .get(fileType.ordinal())
                  .apply("ReadHeader", ParDo.of(new ParquetColumnNamesDoFn()));
          headerList.and(readHeaderParquet);
          break;

        case TSV:
          PCollection<KV<String, List<String>>> readHeaderTsv =
              input
                  .get(fileType.ordinal())
                  .apply("ReadHeader", ParDo.of(new CSVColumnNamesDoFn('\t')));
          headerList.and(readHeaderTsv);
          break;

        case TXT:
          PCollection<KV<String, List<String>>> readHeaderTxt =
              input
                  .get(fileType.ordinal())
                  .apply("ReadHeader", ParDo.of(new TxtColumnNameDoFn(options.getHeaders())));
          headerList.and(readHeaderTxt);
          break;

        default:
          throw new IllegalStateException("Unexpected value: " + fileType);
      }
    }

    readHeader = headerList.apply(Flatten.pCollections())
            .setCoder(KvCoder.of(StringUtf8Coder.of(), ListCoder.of(StringUtf8Coder.of())));

    return readHeader.apply("ViewAsList", View.asMap());
  }

  public static PipelineResult run(InspectClassifyPipelineOptions options) throws CannotProvideCoderException {
    Pipeline p = Pipeline.create(options);
    p.getCoderRegistry().registerCoderForClass(TableRow.class, DeterministicTableRowJsonCoder.of());

    PCollection<KV<String, FileIO.ReadableFile>> inputFiles =
            p.apply(
                    "Read Existing Files",
                    ReadExistingFilesTransform.newBuilder()
                            .setFilePattern(options.getFilePattern())
                            .setProcessExistingFiles(options.getProcessExistingFiles())
                            .build())
            .apply("Fixed Window", Window.into(FixedWindows.of(WINDOW_INTERVAL)));

    /** Create a List of PCollections where list index represents the FileType enum's ordinal */
    PCollectionList<KV<String, FileIO.ReadableFile>> files =
        inputFiles.apply(
            Partition.of(
                Util.FileType.values().length,
                new Partition.PartitionFn<KV<String, FileIO.ReadableFile>>() {
                  @Override
                  public int partitionFor(KV<String, FileIO.ReadableFile> elem, int numPartitions) {
                    String resourceId = String.valueOf(elem.getValue().getMetadata().resourceId());
                    String extension = String.valueOf(resourceId.substring(resourceId.lastIndexOf(".") + 1));
                    LOG.info("Found extension: {} and FileType: {} ", extension, Util.FileType.valueOf(extension.toUpperCase()).toString());
                    return Util.FileType.valueOf(extension.toUpperCase()).ordinal();
                  }
                }));

    /**
     * Store Table.Row objects of every FileType in a Mapping from FileType to PCollection before
     * DLPTransform
     */
    PCollectionList<KV<String, Table.Row>> records = PCollectionList.empty(p);

    List<Util.FileType> fileExtensionList =
        options.getFileTypes().stream()
            .map(x -> Util.FileType.valueOf(x.toUpperCase()))
            .collect(Collectors.toList());

    /**
     * Store header objects of every FileType in a single PCollectionView assuming no two input
     * files can have same name.
     */
     final PCollectionView<Map<String, List<String>>> headers =
            files.apply(
                "Extract Column Names",
                NewExtractColumnNamesTransform.newBuilder()
                    .setHeaders(options.getHeaders())
                    .setColumnDelimiter(options.getColumnDelimiter())
                    .setPubSubGcs(false)
                    .setProjectId(options.getProject())
                    .setFileTypes(fileExtensionList)
                    .build());
//    PCollectionView<Map<String, List<String>>> headers =
//        createHeaders(files, p, fileExtensionList, options);

    for (Util.FileType fileType : fileExtensionList) {
      switch (fileType) {
        case AVRO:
          PCollection<KV<String, FileIO.ReadableFile>> inputFilesavro = files.get(Util.FileType.AVRO.ordinal());
          PCollection<KV<String, Table.Row>> avroRecords = inputFilesavro.apply(ParDo.of(new AvroReaderSplittableDoFn(options.getKeyRange(), options.getSplitSize())))
                                              .setCoder(KvCoder.of(StringUtf8Coder.of(), GenericRecordCoder.of()))
                                              .apply(ParDo.of(new ConvertAvroRecordToDlpRowDoFn()));
          records = records.and(avroRecords);
          break;

        case CSV:
          PCollection<KV<String, FileIO.ReadableFile>> inputFilescsv =
              files.get(Util.FileType.CSV.ordinal());
          PCollection<KV<String, Table.Row>> csvRecords =
              files
                  .get(Util.FileType.CSV.ordinal())
                  .apply(
                      "SplitCSVFile",
                      ParDo.of(
                          new CSVFileReaderSplitDoFn(
                              options.getRecordDelimiter(), options.getSplitSize())))
                  .apply(
                      "ConvertToDLPRow",
                      ParDo.of(new ConvertCSVRecordToDLPRow(options.getColumnDelimiter(), headers))
                          .withSideInputs(headers));

          records = records.and(csvRecords);
          break;

          //                case JSONL:
          //                    records =
          //                            inputFiles
          //                                    .apply(
          //                                            "SplitJSONFile",
          //                                            ParDo.of(
          //                                                    new JsonReaderSplitDoFn(
          //                                                            options.getKeyRange(),
          //
          // options.getRecordDelimiter(),
          //                                                            options.getSplitSize())))
          //                                    .apply("ConvertToDLPRow", ParDo.of(new
          // ConvertJsonRecordToDLPRow()));
          //                    break;
          //                case TXT:
          //                    PCollectionTuple recordTuple =
          //                            inputFiles
          //                                    .apply(
          //                                            "SplitTextFile",
          //                                            ParDo.of(
          //                                                    new TxtReaderSplitDoFn(
          //                                                            options.getKeyRange(),
          //
          // options.getRecordDelimiter(),
          //                                                            options.getSplitSize())))
          //                                    .apply(
          //                                            "ParseTextFile",
          //                                            ParDo.of(new ParseTextLogDoFn())
          //                                                    .withOutputTags(
          //                                                            Util.agentTranscriptTuple,
          //
          // TupleTagList.of(Util.customerTranscriptTuple)));
          //
          //                    records =
          //
          // PCollectionList.of(recordTuple.get(Util.agentTranscriptTuple))
          //                                    .and(recordTuple.get(Util.customerTranscriptTuple))
          //                                    .apply("Flatten", Flatten.pCollections())
          //                                    .apply(
          //                                            "ConvertToDLPRow",
          //                                            ParDo.of(new
          // ConvertTxtToDLPRow(options.getColumnDelimiter(), headers))
          //                                                    .withSideInputs(headers));
          //                    break;
          //                case PARQUET:
          ////      TODO: Remove KeyRange parameter, as it is unused
          //                    records = inputFiles
          //                            .apply(ParDo.of(new
          // ParquetReaderSplittableDoFn(options.getKeyRange(), options.getSplitSize())));
          //                    break;
          //
          //                case ORC:
          //                    records = inputFiles
          //                            .apply("ReadFromORCFilesAsOrcStruct",
          //                                    ParDo.of(new ORCReaderDoFn(options.getProject())));
          //                    break;

        case TEXT:

        default:
          throw new IllegalArgumentException("Please validate FileType parameter");
      }
    }

    PCollection<KV<String, Table.Row>> flattenedRecords = null;
    try {
      flattenedRecords = records.apply(Flatten.pCollections())
              .setCoder(KvCoder.of(StringUtf8Coder.of(), p.getCoderRegistry().getCoder(Table.Row.class)));
    } catch (CannotProvideCoderException e) {
      throw new RuntimeException(e);
    }
    ;

    flattenedRecords
            .apply("DLPInspection", DLPInspectText.newBuilder()
                    .setBatchSizeBytes(options.getBatchSize())
                    .setColumnDelimiter(options.getColumnDelimiter())
                    .setHeaderColumns(headers)
                    .setInspectTemplateName(options.getInspectTemplateName())
                    .setProjectId(options.getDLPParent())
                    .setDlpApiRetryCount(options.getDlpApiRetryCount())
                    .setInitialBackoff(options.getInitialBackoff())
                    .build())
            .apply("ClassifyFiles", ClassifyFiles.newBuilder().setOutputPubSubTopic(options.getTopic()).build());

    return p.run();
  }
}
