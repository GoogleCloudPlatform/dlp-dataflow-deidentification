package com.google.swarm.tokenization.common;

import com.google.auto.value.AutoValue;
import com.google.swarm.tokenization.common.CSVFileReaderTransform.Builder;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Watch;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@AutoValue
public abstract class FileReaderTransform
    extends PTransform<PBegin, PCollection<KV<String, String>>> {

  public static final Logger LOG = LoggerFactory.getLogger(FileReaderTransform.class);
  private static final Duration DEFAULT_POLL_INTERVAL = Duration.standardSeconds(60);

  public abstract String filePattern();

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder setFilePattern(String filePattern);

    public abstract FileReaderTransform build();
  }

  public static Builder newBuilder() {
    return new AutoValue_FileReaderTransform.Builder();
  }

  @Override
  public PCollection<KV<String, String>> expand(PBegin input) {
    return input
        .apply(
            "PollInputFiles",
            FileIO.match()
                .filepattern(filePattern())
                .continuously(DEFAULT_POLL_INTERVAL, Watch.Growth.never()))
        .apply("AutoCompressionCheck", FileIO.readMatches().withCompression(Compression.AUTO))
        .apply("AddFileNameAsKey", ParDo.of(new FileSourceDoFn()))
        .apply("ReadFile", ParDo.of(new FileReaderSplitDoFn()));
  }
}
