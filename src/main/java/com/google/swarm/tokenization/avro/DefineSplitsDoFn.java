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

import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.beam.sdk.io.FileIO.ReadableFile;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Scans all data blocks in the given avro file, then defines "splits" (i.e. block ranges) whose length fits
 * within a configurable byte size.
 */
public class DefineSplitsDoFn extends DoFn<KV<String, ReadableFile>, KV<String, ReadableFile>> {

    public static final Logger LOG = LoggerFactory.getLogger(DefineSplitsDoFn.class);
    public long maxBytesPerSplit;
    public long maxCellsPerSplit;

    public DefineSplitsDoFn(long maxBytesPerSplit, long maxCellsPerSplit) {
        this.maxBytesPerSplit = maxBytesPerSplit;
        this.maxCellsPerSplit = maxCellsPerSplit;
    }

    private void outputValue(ProcessContext c, String fileName, ReadableFile avroFile, long fromPosition, long toPosition) throws IOException {
        String key = String.format("%s~%d~%d",
            fileName,
            fromPosition,
            toPosition
        );
        c.output(KV.of(key, avroFile));
    }

    @ProcessElement
    public void processElement(ProcessContext c) throws IOException {
        String fileName = c.element().getKey();
        ReadableFile file = c.element().getValue();

        try (AvroUtil.AvroSeekableByteChannel channel = AvroUtil.getChannel(file)) {
            DatumReader<GenericRecord> reader = new GenericDatumReader<>();
            DataFileReader<GenericRecord> fileReader = new DataFileReader<>(channel, reader);

            // Move to the first data block
            fileReader.sync(0);

            // Get the current position, which corresponds to the header size.
            long headerOffset = fileReader.tell();

            // Get number of fields/columns, so we can later limit the number of cells per split
            int numFields = fileReader.getSchema().getFields().size();

            // Bail if the file doesn't contain any data
            if (!fileReader.hasNext()) {
                LOG.info("File does not contain any data: {}", fileName);
                return;
            }

            // Initialize the cursors
            long splitStart = 0;
            long splitEnd;
            long byteCounter = 0;
            long cellCounter = 0;

            while (true) {
                // Update the counters
                byteCounter += fileReader.getBlockSize();
                cellCounter += fileReader.getBlockCount() * numFields;

                // Look ahead to the next block
                fileReader.nextBlock();
                long nextBlockSize = fileReader.getBlockSize();
                long nextBlockCells = fileReader.getBlockCount() * numFields;
                splitEnd = fileReader.tell() - headerOffset;

                // Check if we've reached the end of the file
                if (!fileReader.hasNext()) {
                    // Check if there are any un-processed blocks left
                    if (splitEnd != splitStart) {
                        // Create one last split
                        outputValue(c, fileName, file, splitStart, splitEnd);
                    }
                    // End the loop
                    break;
                }

                // Check if we've reached the end of a split
                if (byteCounter + nextBlockSize > maxBytesPerSplit || cellCounter + nextBlockCells > maxCellsPerSplit) {
                    outputValue(c, fileName, file, splitStart, splitEnd);
                    // Reset the counters for the next split
                    splitStart = splitEnd;
                    byteCounter = 0;
                    cellCounter = 0;
                }

            }

            fileReader.close();
        }
    }

}
