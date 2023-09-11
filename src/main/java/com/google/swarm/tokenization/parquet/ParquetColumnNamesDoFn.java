package com.google.swarm.tokenization.parquet;

import java.io.IOException;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.nio.channels.SeekableByteChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.io.FileIO.ReadableFile;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.parquet.io.SeekableInputStream;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.io.DelegatingSeekableInputStream;
import org.apache.parquet.hadoop.ParquetReader;


/** Reads the given parquet file's schema then outputs flattened column names. */
public class ParquetColumnNamesDoFn extends DoFn<KV<String, ReadableFile>, KV<String, List<String>>> {

    public static final Logger LOG = LoggerFactory.getLogger(ParquetColumnNamesDoFn.class);

    @ProcessElement
    public void processElement(ProcessContext c) throws IOException {
        LOG.info("Processing column names for parquet files");
        String fileName = c.element().getKey();
        ReadableFile parquetFile = c.element().getValue();

        SeekableByteChannel seekableByteChannel = parquetFile.openSeekable();
        AvroParquetReader.Builder builder = AvroParquetReader.<GenericRecord>builder(new BeamParquetInputFile(seekableByteChannel));

        try (ParquetReader<GenericRecord> fileReader = builder.build()) {
            GenericRecord record = fileReader.read();
            List<String> flattenedFieldNames = RecordFlattener.forGenericRecord().flattenColumns(record);
            LOG.info("Column names after flattening: ", flattenedFieldNames);
            c.output(KV.of(fileName, flattenedFieldNames));
        }
        seekableByteChannel.close();
    }

    private static class BeamParquetInputFile implements InputFile {

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
}
