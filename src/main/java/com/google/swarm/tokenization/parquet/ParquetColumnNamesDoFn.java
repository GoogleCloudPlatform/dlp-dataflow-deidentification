package com.google.swarm.tokenization.parquet;

import java.io.IOException;
import java.nio.channels.Channels;
import java.util.List;
import java.nio.channels.SeekableByteChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.io.FileIO.ReadableFile;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;


/** Reads the given Parquet file's schema then outputs flattened column names. */
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
            c.output(KV.of(fileName, flattenedFieldNames));
        }
        seekableByteChannel.close();
    }
}
