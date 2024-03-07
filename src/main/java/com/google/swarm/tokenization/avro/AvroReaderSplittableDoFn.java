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
package com.google.swarm.tokenization.avro;

import static org.apache.avro.file.DataFileConstants.SYNC_SIZE;

import java.io.IOException;
import java.util.List;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.io.FileIO.ReadableFile;
import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.splittabledofn.OffsetRangeTracker;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.values.KV;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A splittable DoFn that splits the given Avro file into chunks, then reads the chunks in parallel
 * and outputs all the ingested Avro records.
 */
public class AvroReaderSplittableDoFn
    extends DoFn<KV<String, ReadableFile>, KV<String, GenericRecord>> {

  public static final Logger LOG = LoggerFactory.getLogger(AvroReaderSplittableDoFn.class);
  private final Counter numberOfRowsRead =
      Metrics.counter(AvroReaderSplittableDoFn.class, "numberOfRowsRead");
  private final Integer splitSize;
  private final Integer keyRange;

  public AvroReaderSplittableDoFn(Integer keyRange, Integer splitSize) {
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
    String fileName = c.element().getKey();

    AvroUtil.AvroSeekableByteChannel channel = AvroUtil.getChannel(c.element().getValue());
    DataFileReader<GenericRecord> fileReader =
        new DataFileReader<>(channel, new GenericDatumReader<>());

    long start = tracker.currentRestriction().getFrom();
    long end = tracker.currentRestriction().getTo();

    // Move the first sync point after the
    fileReader.sync(Math.max(start - SYNC_SIZE, 0));

    // Claim the whole split's range
    if (tracker.tryClaim(end - 1)) {
      // Loop through all records in the split. More precisely from the first
      // sync point after the range's start position until the first sync point
      // after the range's end position.
      while (fileReader.hasNext() && !fileReader.pastSync(end - SYNC_SIZE)) {
        // Read the next Avro record in line
        GenericRecord record = fileReader.next();

        // Output the Avro record
        c.outputWithTimestamp(KV.of(fileName, record), Instant.now());
        numberOfRowsRead.inc();
      }
    }

    fileReader.close();
    channel.close();
  }

  @GetInitialRestriction
  public OffsetRange getInitialRestriction(@Element KV<String, ReadableFile> element)
      throws IOException {
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
    for (final OffsetRange p : splits) {
      out.output(p);
    }
  }

  @NewTracker
  public OffsetRangeTracker newTracker(@Restriction OffsetRange range) {
    return new OffsetRangeTracker(new OffsetRange(range.getFrom(), range.getTo()));
  }
}
