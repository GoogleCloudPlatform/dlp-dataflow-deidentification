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

import org.apache.avro.file.SeekableInput;
import org.apache.beam.sdk.io.FileIO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Various helpers for working with Avro files.
 */
public class AvroUtil {

    public static final Logger LOG = LoggerFactory.getLogger(AvroUtil.class);

    /**
     * Byte channel that enables random access for Avro files.
     */
    public static class AvroSeekableByteChannel implements SeekableInput {

        private final SeekableByteChannel channel;

        public AvroSeekableByteChannel(SeekableByteChannel channel) {
            this.channel = channel;
        }

        @Override
        public void seek(long p) throws IOException {
            channel.position(p);
        }

        @Override
        public long tell() throws IOException {
            return channel.position();
        }

        @Override
        public long length() throws IOException {
            return channel.size();
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            ByteBuffer buffer = ByteBuffer.wrap(b, off, len);
            return channel.read(buffer);
        }

        @Override
        public void close() throws IOException {
            channel.close();
        }
    }

    /**
     * Converts a ReadableFile to a byte channel enabled for random access.
     */
    public static AvroSeekableByteChannel getChannel(FileIO.ReadableFile file) {
        SeekableByteChannel channel;
        try {
            channel = file.openSeekable();
        } catch (IOException e) {
            LOG.error("Failed to open file {}", e.getMessage());
            throw new RuntimeException(e);
        }
        return new AvroSeekableByteChannel(channel);
    }

}
