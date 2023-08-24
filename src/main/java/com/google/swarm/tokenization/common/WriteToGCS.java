package com.google.swarm.tokenization.common;

import com.google.auto.value.AutoValue;
import com.google.privacy.dlp.v2.Table;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.WriteFilesResult;
import org.apache.beam.sdk.transforms.Contextful;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;

import java.text.DecimalFormat;
import java.util.List;
import java.util.stream.Collectors;

@AutoValue
@SuppressWarnings("serial")
public abstract class WriteToGCS extends PTransform<PCollection<KV<String, Table.Row>>, WriteFilesResult<String>> {

    public abstract String outputBucket();

    public abstract Util.FileType fileType();

    public static Builder newBuilder() {
        return new AutoValue_WriteToGCS.Builder();
    }

    @AutoValue.Builder
    public abstract static class Builder {
        public abstract Builder setOutputBucket(String value);

        public abstract Builder setFileType(Util.FileType value);

        public abstract WriteToGCS build();
    }

    @Override
    public WriteFilesResult<String> expand(PCollection<KV<String, Table.Row>> input) {

        return input.apply("ConvertTableRowToString",ParDo.of(new ConvertTableRowToString(fileType())))
                .apply("WriteFileToGCS", FileIO.<String, KV<String, String>>writeDynamic()
                    .by(KV::getKey)
                    .withDestinationCoder(StringUtf8Coder.of())
                    .via(Contextful.fn(KV::getValue), TextIO.sink())
                    .to(outputBucket())
                    .withNaming(key -> new CsvFileNaming(key, ".csv"))
                    .withNumShards(1));

    }

    public static class CsvFileNaming implements FileIO.Write.FileNaming {
        private String fileName;
        private String suffix;

        public CsvFileNaming(String fileName, String suffix) {
            this.fileName = fileName;
            this.suffix = suffix;
        }

        @Override
        public @UnknownKeyFor @NonNull @Initialized String getFilename(@UnknownKeyFor @NonNull @Initialized BoundedWindow window, @UnknownKeyFor @NonNull @Initialized PaneInfo pane, @UnknownKeyFor @NonNull @Initialized int numShards, @UnknownKeyFor @NonNull @Initialized int shardIndex, @UnknownKeyFor @NonNull @Initialized Compression compression) {
            StringBuilder res = new StringBuilder(this.fileName);
            String numShardsStr = String.valueOf(numShards);
            DecimalFormat df =
                    new DecimalFormat("000000000000".substring(0, Math.max(5, numShardsStr.length())));
            res.append("-").append(df.format(shardIndex)).append("-of-").append(df.format(numShards));
            res.append(this.suffix);
            return res.toString();
        }
    }

    public class ConvertTableRowToString extends DoFn<KV<String, Table.Row>, KV<String, String>> {

        public Util.FileType fileType;

        public ConvertTableRowToString(Util.FileType fileType) {
            this.fileType = fileType;
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            String fileName = c.element().getKey();
            List<String> stringRow = c.element().getValue().getValuesList().stream().map(e -> e.getStringValue()).collect(Collectors.toList());

            String delimiter = ",";
            switch (fileType){
                case TSV:
                    delimiter = "\t";
                case TXT:
                case JSONL:
                case AVRO:
                case CSV:
                    c.output(KV.of(fileName, String.join(delimiter, stringRow)));
                    break;
            }


        }
    }


}