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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.util.List;
import java.util.Random;

import com.google.protobuf.ByteString;
import com.google.swarm.tokenization.common.FileReader;
import com.google.swarm.tokenization.avro.AvroUtil.AvroSeekableByteChannel;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
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
import static org.apache.avro.file.DataFileConstants.SYNC_SIZE;

/**
 * A SplitDoFn that splits the given Avro file into chunks. Each chunks contains an Avro data block, which
 * themselves contain some Avro records.
 * Each output contains a copy of the Avro file header, followed by the block's data bytes. The header is
 * needed is downstream steps to consume the data using the Avro Java library. Without the header the library can't
 * read the data.
 */
public class AvroReaderSplitDoFn extends DoFn<KV<String, ReadableFile>, KV<String, ByteString>> {

  public static final Logger LOG = LoggerFactory.getLogger(AvroReaderSplitDoFn.class);
  private final Counter numberOfAvroBlocksScanned =
      Metrics.counter(AvroReaderSplitDoFn.class, "numberOfAvroBlocksScanned");
  private final Integer splitSize;
  private final Integer keyRange;

  public AvroReaderSplitDoFn(Integer keyRange, Integer splitSize) {
    this.keyRange = keyRange;
    this.splitSize = splitSize;
  }

  /**
   * Returns the sync marker used by the given Avro file.
   * Each Avro file has its own randomly-generated sync marker that separates every data block.
   */
  public static ByteString extractSyncMarker(ReadableFile file) throws IOException {
    try (AvroSeekableByteChannel channel = AvroUtil.getChannel(file)) {
      DatumReader<GenericRecord> reader = new GenericDatumReader<>();
      DataFileReader<GenericRecord> fileReader = new DataFileReader<>(channel, reader);

      // Move to the first data block
      fileReader.sync(0);

      // Create a buffer for the syn marker
      ByteBuffer buffer = ByteBuffer.allocate(SYNC_SIZE);

      // Move back by SYNC_SIZE (16) bytes
      channel.seek(channel.tell() - SYNC_SIZE);

      // Read the sync marker into the buffer
      channel.read(buffer);
      fileReader.close();
      return ByteString.copyFrom(buffer.array());
    }
  }

  /**
   * Returns the position (in bytes) where the first data block starts,
   * i.e after the Avro header and initial sync marker.
   */
  public static long getFirstDataBlockPosition(ReadableFile file) throws IOException {
    try (AvroSeekableByteChannel channel = AvroUtil.getChannel(file)) {
      DatumReader<GenericRecord> reader = new GenericDatumReader<>();
      DataFileReader<GenericRecord> fileReader = new DataFileReader<>(channel, reader);
      // Move to first data block
      fileReader.sync(0);
      // Return position
      return fileReader.tell();
    }
  }

  @ProcessElement
  public void processElement(ProcessContext c, RestrictionTracker<OffsetRange, Long> tracker)
      throws IOException {
    String fileName = c.element().getKey();
    ByteString syncMarker = extractSyncMarker(c.element().getValue());

    try (SeekableByteChannel channel = getReader(c.element().getValue())) {
      FileReader reader = new FileReader(channel, tracker.currentRestriction().getFrom(), syncMarker.toByteArray());
      while (tracker.tryClaim(reader.getStartOfNextRecord())) {
        // Get the Avro data bytes for the current block
        reader.readNextRecord();
        ByteString avroBlock = reader.getCurrent().concat(syncMarker);
        String key = String.format("%s~%d", fileName, new Random().nextInt(keyRange));
        numberOfAvroBlocksScanned.inc();
        c.outputWithTimestamp(KV.of(key, avroBlock), Instant.now());
      }
    }
  }

  @GetInitialRestriction
  public OffsetRange getInitialRestriction(@Element KV<String, ReadableFile> file)
      throws IOException {
    long totalBytes = file.getValue().getMetadata().sizeBytes();
    long firstDataBlockPosition = getFirstDataBlockPosition(file.getValue());
    LOG.info("Initial Restriction range from {} to: {}", firstDataBlockPosition, totalBytes);
    return new OffsetRange(firstDataBlockPosition, totalBytes);
  }

  @SplitRestriction
  public void splitRestriction(
      @Element KV<String, ReadableFile> file,
      @Restriction OffsetRange range,
      OutputReceiver<OffsetRange> out) {
    List<OffsetRange> splits = range.split(splitSize, splitSize);
    LOG.info("Number of Splits: {}", splits.size());
    for (final OffsetRange p : splits) {
      out.output(p);
    }
  }

  @NewTracker
  public OffsetRangeTracker newTracker(@Restriction OffsetRange range) {
    return new OffsetRangeTracker(new OffsetRange(range.getFrom(), range.getTo()));
  }

  private static SeekableByteChannel getReader(ReadableFile eventFile) {
    SeekableByteChannel channel;
    try {
      channel = eventFile.openSeekable();
    } catch (IOException e) {
      LOG.error("Failed to open file {}", e.getMessage());
      throw new RuntimeException(e);
    }
    return channel;
  }
}
