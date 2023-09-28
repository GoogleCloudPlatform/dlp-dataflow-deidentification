package com.google.swarm.tokenization.parquet;

import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.channels.SeekableByteChannel;
import org.apache.parquet.io.SeekableInputStream;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.DelegatingSeekableInputStream;

public class BeamParquetInputFile implements InputFile {

    private SeekableByteChannel seekableByteChannel;

    BeamParquetInputFile(SeekableByteChannel seekableByteChannel) {
        this.seekableByteChannel = seekableByteChannel;
    }

    @Override
    public long getLength() throws IOException {
        return seekableByteChannel.size();
    }

    @Override
    public SeekableInputStream newStream() {
        return new DelegatingSeekableInputStream(Channels.newInputStream(seekableByteChannel)) {

            @Override
            public long getPos() throws IOException {
                return seekableByteChannel.position();
            }

            @Override
            public void seek(long newPos) throws IOException {
                seekableByteChannel.position(newPos);
            }
        };
    }
}
