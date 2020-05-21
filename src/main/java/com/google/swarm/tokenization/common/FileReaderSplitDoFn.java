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

import com.google.protobuf.ByteString;

import java.io.IOException;
import java.io.FileNotFoundException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.util.Arrays;

import org.apache.beam.sdk.io.FileIO.ReadableFile;
import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.splittabledofn.OffsetRangeTracker;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileReaderSplitDoFn extends DoFn<KV<String, ReadableFile>, KV<String, String>> {

    public static final Logger LOG = LoggerFactory.getLogger(FileReaderSplitDoFn.class);
    public static Integer SPLIT_SIZE = 900000;

    public Integer BATCH_SIZE;

    public FileReaderSplitDoFn(Integer batchSize) {
        this.BATCH_SIZE = batchSize;
    }
    

    @ProcessElement
    public void processElement(ProcessContext c, RestrictionTracker<OffsetRange, Long> tracker)
            throws IOException {
        String fileName = c.element().getKey();

        try (SeekableByteChannel channel = getReader(c.element().getValue())) {
            ByteBuffer readBuffer = ByteBuffer.allocate(BATCH_SIZE);
            ByteString buffer = ByteString.EMPTY;
            for (long i = tracker.currentRestriction().getFrom(); tracker.tryClaim(i); ++i) {
                long startOffset = (i * BATCH_SIZE) - BATCH_SIZE;
                channel.position(startOffset);
                readBuffer = ByteBuffer.allocate(BATCH_SIZE);
                buffer = ByteString.EMPTY;
                channel.read(readBuffer);
                readBuffer.flip();
                buffer = ByteString.copyFrom(readBuffer);
                readBuffer.clear();
                LOG.debug(
                        "File Read Transform:ReadFile: Current Restriction {}, Content Size{}", tracker.currentRestriction(), buffer.size());
                c.output(KV.of(fileName, buffer.toStringUtf8().trim()));
            }
        } catch (Exception e) {
            LOG.error("File Read Transform:ReadFile: Error processing the file - " + Arrays.toString(e.getStackTrace()));
        }
    }

    @GetInitialRestriction
    public OffsetRange getInitialRestriction(@Element KV<String, ReadableFile> file)
            throws IOException {
        long totalBytes = file.getValue().getMetadata().sizeBytes();
        long totalSplit = 0;
        if (totalBytes < BATCH_SIZE) {
            totalSplit = 2;
        } else {
            totalSplit = totalSplit + (totalBytes / BATCH_SIZE);
            long remaining = totalBytes % BATCH_SIZE;
            if (remaining > 0) {
                totalSplit = totalSplit + 2;
            }
        }

        LOG.info(
                "File Read Transform:ReadFile: Total Bytes {} for File {} -Initial Restriction range from 1 to: {}. {} chunk/(s) created of size {} bytes",
                totalBytes,
                file.getKey(),
                totalSplit,
                totalSplit - 1,
                BATCH_SIZE);
        return new OffsetRange(1, totalSplit);
    }

    @SplitRestriction
    public void splitRestriction(
            @Element KV<String, ReadableFile> file, @Restriction OffsetRange range, OutputReceiver<OffsetRange> out) {
        for (final OffsetRange p : range.split(1, 1)) {
            out.output(p);
        }
    }

    @NewTracker
    public OffsetRangeTracker newTracker(@Restriction OffsetRange range) {
        return new OffsetRangeTracker(new OffsetRange(range.getFrom(), range.getTo()));
    }

    private static SeekableByteChannel getReader(ReadableFile eventFile)
            throws IOException, FileNotFoundException {
        SeekableByteChannel channel = null;
        channel = eventFile.openSeekable();
        return channel;
    }

}

