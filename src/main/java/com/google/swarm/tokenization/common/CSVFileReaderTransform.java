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
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@AutoValue
public abstract class CSVFileReaderTransform
    extends PTransform<PBegin, PCollection<KV<String, Iterable<ReadableFile>>>> {
  public static final Logger LOG = LoggerFactory.getLogger(CSVFileReaderTransform.class);

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
        .apply("Poll Input Files", FileIO.match().filepattern(csvFilePattern()))
        .apply("Find Pattern Match", FileIO.readMatches().withCompression(Compression.AUTO))
        .apply("Add File Name as Key", WithKeys.of(file -> Util.getFileName(file)))
        .setCoder(KvCoder.of(StringUtf8Coder.of(), ReadableFileCoder.of()))
        .apply(GroupByKey.create());
  }
}
