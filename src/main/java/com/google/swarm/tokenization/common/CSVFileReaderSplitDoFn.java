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
package com.google.swarm.tokenization.common;

import java.io.IOException;
import java.nio.channels.SeekableByteChannel;
import java.util.List;
import org.apache.beam.sdk.io.FileIO.ReadableFile;
import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.splittabledofn.OffsetRangeTracker;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("serial")
public class CSVFileReaderSplitDoFn extends DoFn<KV<String, ReadableFile>, KV<String, String>> {
  public static final Logger LOG = LoggerFactory.getLogger(CSVFileReaderSplitDoFn.class);
  private final Counter numberOfRowsRead =
      Metrics.counter(CSVFileReaderSplitDoFn.class, "numberOfRowsRead");

  private final String recordDelimiter;
  private final Integer splitSize;

  public CSVFileReaderSplitDoFn(String recordDelimiter, Integer splitSize) {
    this.recordDelimiter = recordDelimiter;
    this.splitSize = splitSize;
  }

  @ProcessElement
  public void processElement(ProcessContext c, RestrictionTracker<OffsetRange, Long> tracker)
      throws IOException {
    String fileName = c.element().getKey();
    try (SeekableByteChannel channel = getReader(c.element().getValue())) {
      FileReader reader =
          new FileReader(
              channel, tracker.currentRestriction().getFrom(), recordDelimiter.getBytes());
      while (tracker.tryClaim(reader.getStartOfNextRecord())) {
        Long readerPosition = reader.getStartOfNextRecord();
        reader.readNextRecord();
        if (readerPosition == 0) {
          continue;
        }
        String contents = reader.getCurrent().toStringUtf8();
        String key = fileName;
        numberOfRowsRead.inc();
        c.outputWithTimestamp(KV.of(key, contents), c.timestamp());
      }
    }
  }

  @GetInitialRestriction
  public OffsetRange getInitialRestriction(@Element KV<String, ReadableFile> file)
      throws IOException {
    long totalBytes = file.getValue().getMetadata().sizeBytes();
    LOG.info("Initial Restriction range from 1 to: {}", totalBytes);
    return new OffsetRange(0, totalBytes);
  }

  @SplitRestriction
  public void splitRestriction(
      @Element KV<String, ReadableFile> file,
      @Restriction OffsetRange range,
      OutputReceiver<OffsetRange> out) {
    long totalBytes = file.getValue().getMetadata().sizeBytes();
    List<OffsetRange> splits = range.split(splitSize, splitSize);
    LOG.info("Number of Splits: {} - Total bytes: {}", splits.size(), totalBytes);
    for (final OffsetRange p : splits) {
      out.output(p);
    }
  }

  @NewTracker
  public OffsetRangeTracker newTracker(@Restriction OffsetRange range) {
    return new OffsetRangeTracker(new OffsetRange(range.getFrom(), range.getTo()));
  }

  private static SeekableByteChannel getReader(ReadableFile eventFile) {
    SeekableByteChannel channel = null;
    try {
      channel = eventFile.openSeekable();
    } catch (IOException e) {
      LOG.error("Failed to Open File {}", e.getMessage());
      throw new RuntimeException(e);
    }
    return channel;
  }
}
