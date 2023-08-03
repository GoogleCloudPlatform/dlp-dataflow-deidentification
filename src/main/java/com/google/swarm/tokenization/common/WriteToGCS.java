package com.google.swarm.tokenization.common;

import com.google.auto.value.AutoValue;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.WriteFilesResult;
import org.apache.beam.sdk.transforms.Contextful;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;

import java.text.DecimalFormat;

@AutoValue
@SuppressWarnings("serial")
public abstract class WriteToGCS extends PTransform<PCollection<KV<String, String>>, WriteFilesResult<String>> {

    public abstract String outputBucket();

    public static Builder newBuilder() {
        return new AutoValue_WriteToGCS.Builder();
    }

    @AutoValue.Builder
    public abstract static class Builder {
        public abstract Builder setOutputBucket(String value);

        public abstract WriteToGCS build();
    }

    @Override
    public WriteFilesResult<String> expand(PCollection<KV<String, String>> input) {
        return input.apply("Write to CSV", FileIO.<String, KV<String, String>>writeDynamic()
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
            // A trillion shards per window per pane ought to be enough for everybody.
            DecimalFormat df =
                    new DecimalFormat("000000000000".substring(0, Math.max(5, numShardsStr.length())));
            res.append("-").append(df.format(shardIndex)).append("-of-").append(df.format(numShards));
            res.append(this.suffix);
            return res.toString();
        }
    }
}