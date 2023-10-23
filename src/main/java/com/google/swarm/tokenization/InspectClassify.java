package com.google.swarm.tokenization;

import com.google.api.services.bigquery.model.TableRow;
import com.google.privacy.dlp.v2.Table;
import com.google.swarm.tokenization.avro.AvroReaderSplittableDoFn;
import com.google.swarm.tokenization.avro.ConvertAvroRecordToDlpRowDoFn;
import com.google.swarm.tokenization.avro.GenericRecordCoder;
import com.google.swarm.tokenization.beam.ConvertCSVRecordToDLPRow;
import com.google.swarm.tokenization.classification.FileKeyObject;
import com.google.swarm.tokenization.classification.SanitizeFileKeyDoFn;
import com.google.swarm.tokenization.coders.DeterministicTableRowJsonCoder;
import com.google.swarm.tokenization.common.*;
import com.google.swarm.tokenization.json.ConvertJsonRecordToDLPRow;
import com.google.swarm.tokenization.json.JsonReaderSplitDoFn;
import com.google.swarm.tokenization.orc.ORCReaderDoFn;
import com.google.swarm.tokenization.parquet.ParquetReaderSplittableDoFn;
import com.google.swarm.tokenization.txt.ConvertTxtToDLPRow;
import com.google.swarm.tokenization.txt.ParseTextLogDoFn;
import com.google.swarm.tokenization.txt.TxtReaderSplitDoFn;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.*;
import org.apache.commons.collections.map.HashedMap;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class InspectClassify {

    public static final Logger LOG = LoggerFactory.getLogger(InspectClassify.class);
    private static final Duration DEFAULT_POLL_INTERVAL = Duration.standardSeconds(3);
    private static final Duration WINDOW_INTERVAL = Duration.standardSeconds(3);
    /**
     * PubSub configuration for default batch size in number of messages
     */
    public static final Integer PUB_SUB_BATCH_SIZE = 1000;
    /**
     * PubSub configuration for default batch size in bytes
     */
    public static final Integer PUB_SUB_BATCH_SIZE_BYTES = 10000;

    public static void main(String[] args) {

        InspectClassifyPipelineOptions options =
                PipelineOptionsFactory.fromArgs(args).as(InspectClassifyPipelineOptions.class);
        run(options);
    }

    public static PipelineResult run(InspectClassifyPipelineOptions options) {
        Pipeline p = Pipeline.create(options);
        p.getCoderRegistry().registerCoderForClass(TableRow.class, DeterministicTableRowJsonCoder.of());

        PCollection<KV<FileKeyObject, FileIO.ReadableFile>> inputFiles = p.apply("MatchExistingFiles", FileIO.match().filepattern(options.getFilePattern()))
                .apply("ReadExistingFiles", FileIO.readMatches())
                .apply("SanitizeFileNameExistingFiles", ParDo.of(new SanitizeFileKeyDoFn(Util.InputLocation.GCS)))
                .apply("Fixed Window", Window.into(FixedWindows.of(WINDOW_INTERVAL)));


        PCollectionList<KV<FileKeyObject, FileIO.ReadableFile>> files = inputFiles.apply(Partition.of(
                Util.FileType.values().length,
                new Partition.PartitionFn<KV<FileKeyObject, FileIO.ReadableFile>>(){
                    @Override
                    public int partitionFor(KV<FileKeyObject, FileIO.ReadableFile> elem, int numPartitions) {
                        return elem.getKey().getExtension().ordinal();
                    }

                }
        ));


        Map<Util.FileType,PCollection<KV<String, Table.Row>>> records = new HashMap();
        PCollectionView<Map<String, List<String>>> headers;


        List<Util.FileType> fileExtensionList = options.getFileTypes().stream().map(x -> Util.FileType.valueOf(x.toUpperCase())).collect(Collectors.toList());
        for(Util.FileType fileType: fileExtensionList) {
            switch (fileType) {
//                case AVRO:
//                    records =
//                            files.get(Util.FileType.AVRO.ordinal()).apply("converttoStringkey", MapElements.via(
//                                            new SimpleFunction<KV<FileKeyObject, FileIO.ReadableFile>, KV<String, FileIO.ReadableFile>>() {
//                                                @Override
//                                                public KV<String, FileIO.ReadableFile> apply(KV<FileKeyObject, FileIO.ReadableFile> ele){
//                                                        return KV.of(ele.getKey().getFilename(),ele.getValue());
//                                                }
//
//                                            }
//                                    ))
//                                    .apply(
//                                            ParDo.of(
//                                                    new AvroReaderSplittableDoFn(
//                                                            options.getKeyRange(), options.getSplitSize())))
//                                    .setCoder(KvCoder.of(StringUtf8Coder.of(), GenericRecordCoder.of()))
//                                    .apply(ParDo.of(new ConvertAvroRecordToDlpRowDoFn()));
//                    break;

                case CSV:
                    PCollection<KV<String, FileIO.ReadableFile>> inputFilescsv = files.get(Util.FileType.CSV.ordinal()).apply("convertStringKeyCSV", MapElements.via(
                            new SimpleFunction<KV<FileKeyObject, FileIO.ReadableFile>, KV<String, FileIO.ReadableFile>>() {
                                @Override
                                public KV<String, FileIO.ReadableFile> apply(KV<FileKeyObject, FileIO.ReadableFile> ele) {
                                    return KV.of(ele.getKey().getFilename(), ele.getValue());
                                }
                            }
                    ));
                     PCollectionView<Map<String, List<String>>> csvheaders =
                            inputFilescsv.apply(
                                    "Extract Column Names",
                                    ExtractColumnNamesTransform.newBuilder()
                                            .setFileType(options.getFileType())
                                            .setHeaders(options.getHeaders())
                                            .setColumnDelimiter(options.getColumnDelimiter())
                                            .setPubSubGcs(usePubSub)
                                            .setProjectId(options.getProject())
                                            .build());
                    PCollection<KV<String, Table.Row>> csvRecords =  inputFilescsv.apply(
                            "SplitCSVFile",
                            ParDo.of(
                                    new CSVFileReaderSplitDoFn(
                                            options.getRecordDelimiter(), options.getSplitSize())))
                    .apply(
                        "ConvertToDLPRow",
                        ParDo.of(new ConvertCSVRecordToDLPRow(options.getColumnDelimiter(), headers))
                                .withSideInputs(headers));

                    records.put(Util.FileType.CSV,csvRecords);
                    break;

//                case JSONL:
//                    records =
//                            inputFiles
//                                    .apply(
//                                            "SplitJSONFile",
//                                            ParDo.of(
//                                                    new JsonReaderSplitDoFn(
//                                                            options.getKeyRange(),
//                                                            options.getRecordDelimiter(),
//                                                            options.getSplitSize())))
//                                    .apply("ConvertToDLPRow", ParDo.of(new ConvertJsonRecordToDLPRow()));
//                    break;
//                case TXT:
//                    PCollectionTuple recordTuple =
//                            inputFiles
//                                    .apply(
//                                            "SplitTextFile",
//                                            ParDo.of(
//                                                    new TxtReaderSplitDoFn(
//                                                            options.getKeyRange(),
//                                                            options.getRecordDelimiter(),
//                                                            options.getSplitSize())))
//                                    .apply(
//                                            "ParseTextFile",
//                                            ParDo.of(new ParseTextLogDoFn())
//                                                    .withOutputTags(
//                                                            Util.agentTranscriptTuple,
//                                                            TupleTagList.of(Util.customerTranscriptTuple)));
//
//                    records =
//                            PCollectionList.of(recordTuple.get(Util.agentTranscriptTuple))
//                                    .and(recordTuple.get(Util.customerTranscriptTuple))
//                                    .apply("Flatten", Flatten.pCollections())
//                                    .apply(
//                                            "ConvertToDLPRow",
//                                            ParDo.of(new ConvertTxtToDLPRow(options.getColumnDelimiter(), headers))
//                                                    .withSideInputs(headers));
//                    break;
//                case PARQUET:
////      TODO: Remove KeyRange parameter, as it is unused
//                    records = inputFiles
//                            .apply(ParDo.of(new ParquetReaderSplittableDoFn(options.getKeyRange(), options.getSplitSize())));
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

        for(Map.Entry<Util.FileType,PCollection<KV<String, Table.Row>>> fileRecords :  records.entrySet()){
            PCollectionTuple inspectDeidRecords = fileRecords.getValue().apply(
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
                            .build())
                    .get(Util.inspectOrDeidSuccess)
                    .apply(
                            "InsertToBQ",
                            BigQueryDynamicWriteTransform.newBuilder()
                                    .setDatasetId(options.getDataset())
                                    .setProjectId(options.getProject())
                                    .build());
        }

        return p.run();
    }



}

