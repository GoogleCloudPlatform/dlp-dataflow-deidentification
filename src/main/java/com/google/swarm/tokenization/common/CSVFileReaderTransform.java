package com.google.swarm.tokenization.common;

import com.google.auto.value.AutoValue;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.FileIO.ReadableFile;
import org.apache.beam.sdk.io.ReadableFileCoder;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.Watch;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.transforms.WithTimestamps;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@AutoValue
public abstract class CSVFileReaderTransform
    extends PTransform<PBegin, PCollection<KV<String, Iterable<ReadableFile>>>> {
  public static final Logger LOG = LoggerFactory.getLogger(CSVFileReaderTransform.class);
  private static final Duration DEFAULT_POLL_INTERVAL = Duration.standardSeconds(10);

  public abstract String csvFilePattern();

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder setCsvFilePattern(String csvFilePattern);

    public abstract CSVFileReaderTransform build();
  }

  public static Builder newBuilder() {
    return new AutoValue_CSVFileReaderTransform.Builder();
  }

  @Override
  public PCollection<KV<String, Iterable<ReadableFile>>> expand(PBegin input) {
    return input
        .apply(
            "Poll Input Files",
            FileIO.match()
                .filepattern(csvFilePattern())
                .continuously(DEFAULT_POLL_INTERVAL, Watch.Growth.afterTimeSinceNewOutput(Duration.standardHours(1))))
        .apply("Find Pattern Match", FileIO.readMatches().withCompression(Compression.AUTO))
        .apply("Add File Name as Key", WithKeys.of(file -> Util.getFileName(file)))
        .setCoder(KvCoder.of(StringUtf8Coder.of(), ReadableFileCoder.of()))
        .apply(
            "AssignEventTimestamp",
            WithTimestamps.of((KV<String, ReadableFile> rec) -> Instant.now()))
        .apply(
            "Fixed Window",
            Window.<KV<String, ReadableFile>>into(FixedWindows.of(Duration.standardSeconds(1)))
                .triggering(AfterWatermark.pastEndOfWindow())
                .discardingFiredPanes()
                .withAllowedLateness(Duration.ZERO))
        .apply(GroupByKey.create());
  }
}
