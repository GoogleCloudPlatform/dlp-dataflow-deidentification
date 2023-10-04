package com.google.swarm.tokenization.common;

import com.google.auto.value.AutoValue;
import com.google.privacy.dlp.v2.Table;
import com.google.swarm.tokenization.orc.ORCWriterDoFn;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.WriteFilesResult;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;

import java.io.IOException;
import java.nio.channels.WritableByteChannel;
import java.text.DecimalFormat;
import java.util.List;
import java.util.stream.Collectors;

@AutoValue
@SuppressWarnings("serial")
public abstract class WriteToGCS extends PTransform<PCollection<KV<String, Table.Row>>, WriteFilesResult<String>> {

    public abstract String outputBucket();

    public abstract Util.FileType fileType();

    public abstract Character columnDelimiter();

    public abstract String schema();

    public static Builder newBuilder() {
        return new AutoValue_WriteToGCS.Builder();
    }

    @AutoValue.Builder
    public abstract static class Builder {
        public abstract Builder setOutputBucket(String value);

        public abstract Builder setFileType(Util.FileType value);

        public abstract Builder setColumnDelimiter(Character columnDelimiter);

        public abstract Builder setSchema(String schema);

        public abstract WriteToGCS build();
    }

    @Override
    public WriteFilesResult<String> expand(PCollection<KV<String, Table.Row>> input) {
        switch (fileType()) {
            case ORC:
                input
                    .apply(GroupByKey.create())
                    .apply("WriteORCToGCS",
                            ParDo.of(
                                    new ORCWriterDoFn("gs://dlp-orc-support-398810-demo-data/output",
                                                    TypeDescription.fromString(schema()))))
                    .setCoder(StringUtf8Coder.of());
            case AVRO:
            case CSV:
            case JSONL:
            case PARQUET:
            case TSV:
            case TXT:
            default:
                return input.apply("ConvertTableRowToString", ParDo.of(new ConvertTableRowToString(fileType(), columnDelimiter())))
                        .apply("WriteFileToGCS", FileIO.<String, KV<String, String>>writeDynamic()
                                .by(KV::getKey)
                                .withDestinationCoder(StringUtf8Coder.of())
                                .via(Contextful.fn(KV::getValue), TextIO.sink())
                                .to(outputBucket())
                                .withNaming(key -> new CsvFileNaming(key, ".csv"))
                                .withNumShards(1));
        }
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
        public Character delimiter;

        public ConvertTableRowToString(Util.FileType fileType, Character delimiter) {
            this.fileType = fileType;
            this.delimiter = delimiter;
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            String fileName = c.element().getKey();
            List<String> stringRow = c.element().getValue().getValuesList().stream().map(e -> e.getStringValue()).collect(Collectors.toList());

            c.output(KV.of(fileName, String.join(delimiter.toString(), stringRow)));
        }
    }

    public class ORCSink implements FileIO.Sink<Table.Row> {

        public String filename;
        public Path filePath = new Path("gs://dlp-orc-support-398810-demo-data/output/new_orc.orc");
        public TypeDescription schema = TypeDescription.fromString("struct<column_name1:int,column_name2:int>");
        public Writer writer;
        public LongColumnVector x;
        public LongColumnVector y;
        public VectorizedRowBatch batch;

        public ORCSink(String filename) {
            this.filename = filename;
        }

        @Override
        public void open(@UnknownKeyFor @NonNull @Initialized WritableByteChannel channel) throws @UnknownKeyFor@NonNull@Initialized IOException {
            Configuration conf = new Configuration();
            writer = OrcFile.createWriter(filePath, OrcFile.writerOptions(conf).setSchema(schema));
            batch = schema.createRowBatch();
            x = (LongColumnVector) batch.cols[0];
            y = (LongColumnVector) batch.cols[1];
        }

        @Override
        public void write(Table.Row element) throws @UnknownKeyFor@NonNull@Initialized IOException {
            int row = batch.size++;
            x.vector[row] = 1;
            y.vector[row] = 2;
        }

        @Override
        public void flush() throws @UnknownKeyFor@NonNull@Initialized IOException {
            writer.close();
        }
    }

}