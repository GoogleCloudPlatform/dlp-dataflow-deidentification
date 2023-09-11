package com.google.swarm.tokenization.parquet;

import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.splittabledofn.OffsetRangeTracker;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import java.io.IOException;
import org.joda.time.Instant;
import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.io.FileIO.ReadableFile;
import java.nio.channels.SeekableByteChannel;
import java.nio.channels.Channels;
import java.util.List;
import java.util.Objects;
import org.apache.parquet.io.SeekableInputStream;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.io.DelegatingSeekableInputStream;
import org.apache.parquet.hadoop.ParquetReader;

import com.google.privacy.dlp.v2.Table;

import static org.apache.avro.file.DataFileConstants.SYNC_SIZE;

/**
 * A Splittable DoFn that splits Parquet file into chunks, then read the chunks in parallel and outputs all the ingested
 * Parquet records.
 */
public class ParquetReaderSplittableDoFn extends DoFn<KV<String, ReadableFile>, KV<String, Table.Row>> {

    public static final Logger LOG = LoggerFactory.getLogger(ParquetReaderSplittableDoFn.class);
    private final Counter numberOfParquetRecordsIngested =
            Metrics.counter(ParquetReaderSplittableDoFn.class, "numberOfParquetRecordsIngested");
    private final Integer splitSize;
    private final Integer keyRange;

    public ParquetReaderSplittableDoFn(Integer keyRange, Integer splitSize) {
        this.keyRange = keyRange;
        this.splitSize = splitSize;
    }

    @ProcessElement
    public void processElement(ProcessContext c, RestrictionTracker<OffsetRange, Long> tracker) throws IOException {
        LOG.info("Processing split from {} to {}",
                tracker.currentRestriction().getFrom(),
                tracker.currentRestriction().getTo());

        String fileName = Objects.requireNonNull(c.element().getKey());
        ReadableFile readableFile = Objects.requireNonNull(c.element().getValue());

        long start = tracker.currentRestriction().getFrom();
        long end = tracker.currentRestriction().getTo();

        if (tracker.tryClaim(end-1)) {
            SeekableByteChannel seekableByteChannel = readableFile.openSeekable();
            AvroParquetReader.Builder builder = AvroParquetReader.<GenericRecord>builder(
                    new BeamParquetInputFile(seekableByteChannel));

            try(ParquetReader<GenericRecord> fileReader = builder.withFileRange(start, end).build()) {
                GenericRecord record;
                while ((record = fileReader.read()) != null) {
                    Table.Row tableRow = RecordFlattener.forGenericRecord().flatten(record);
                    if (fileReader.getCurrentRowIndex() == 0)
                        LOG.info("0th index: {}", tableRow.getValuesList());
                    c.outputWithTimestamp(KV.of(fileName, tableRow), Instant.now());
                    numberOfParquetRecordsIngested.inc();
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
    public OffsetRange getInitialRestriction(@Element KV<String, ReadableFile> element){
        long totalBytes = element.getValue().getMetadata().sizeBytes();
        LOG.info("Initial Restriction range from {} to {}", 0, totalBytes);
        return new OffsetRange(0, totalBytes);
    }

    @SplitRestriction
    public void splitRestriction(@Element KV<String, ReadableFile> file,
                                 @Restriction OffsetRange range,
                                 OutputReceiver<OffsetRange> out) {
        List<OffsetRange> splits = range.split(splitSize, splitSize);
        for (final OffsetRange offsetRange:splits) {
            out.output(offsetRange);
        }
    }

    @NewTracker
    public OffsetRangeTracker newTracker(@Restriction OffsetRange range) {
        return new OffsetRangeTracker(new OffsetRange(range.getFrom(), range.getTo()));
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
