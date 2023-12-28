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
package com.google.swarm.tokenization.parquet;

import com.google.privacy.dlp.v2.Table;
import java.io.IOException;
import java.nio.channels.SeekableByteChannel;
import java.util.List;
import java.util.Objects;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.io.FileIO.ReadableFile;
import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.splittabledofn.OffsetRangeTracker;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.values.KV;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A Splittable DoFn that splits Parquet file into chunks, then read the chunks in parallel and
 * outputs all the ingested Parquet records.
 */
public class ParquetReaderSplittableDoFn
    extends DoFn<KV<String, ReadableFile>, KV<String, Table.Row>> {

  public static final Logger LOG = LoggerFactory.getLogger(ParquetReaderSplittableDoFn.class);
  private final Counter numberOfRowsRead =
      Metrics.counter(ParquetReaderSplittableDoFn.class, "numberOfRowsRead");
  private final Integer splitSize;
  private final Integer keyRange;

  public ParquetReaderSplittableDoFn(Integer keyRange, Integer splitSize) {
    this.keyRange = keyRange;
    this.splitSize = splitSize;
  }

  @ProcessElement
  public void processElement(ProcessContext c, RestrictionTracker<OffsetRange, Long> tracker)
      throws IOException {
    LOG.info(
        "Processing split from {} to {}",
        tracker.currentRestriction().getFrom(),
        tracker.currentRestriction().getTo());

    String fileName = Objects.requireNonNull(c.element().getKey());
    ReadableFile readableFile = Objects.requireNonNull(c.element().getValue());

    long start = tracker.currentRestriction().getFrom();
    long end = tracker.currentRestriction().getTo();

    if (tracker.tryClaim(end - 1)) {
      SeekableByteChannel seekableByteChannel = readableFile.openSeekable();
      AvroParquetReader.Builder builder =
          AvroParquetReader.<GenericRecord>builder(new BeamParquetInputFile(seekableByteChannel));

      try (ParquetReader<GenericRecord> fileReader = builder.withFileRange(start, end).build()) {
        GenericRecord record;
        while ((record = fileReader.read()) != null) {
          Table.Row tableRow = RecordFlattener.forGenericRecord().flatten(record);
          c.outputWithTimestamp(KV.of(fileName, tableRow), Instant.now());
          numberOfRowsRead.inc();
        }
      }
      seekableByteChannel.close();
    }
  }

  /***
   * Create initial restriction for SplittableDoFn
   * @param element Mapping of file's name to its readable object
   * @return initial partition
   */
  @GetInitialRestriction
  public OffsetRange getInitialRestriction(@Element KV<String, ReadableFile> element) {
    long totalBytes = element.getValue().getMetadata().sizeBytes();
    LOG.info("Initial Restriction range from {} to {}", 0, totalBytes);
    return new OffsetRange(0, totalBytes);
  }

  @SplitRestriction
  public void splitRestriction(
      @Element KV<String, ReadableFile> file,
      @Restriction OffsetRange range,
      OutputReceiver<OffsetRange> out) {
    List<OffsetRange> splits = range.split(splitSize, splitSize);
    LOG.info("Number of splits: {}", splits.size());
    for (final OffsetRange offsetRange : splits) {
      out.output(offsetRange);
    }
  }

  @NewTracker
  public OffsetRangeTracker newTracker(@Restriction OffsetRange range) {
    return new OffsetRangeTracker(new OffsetRange(range.getFrom(), range.getTo()));
  }
}
