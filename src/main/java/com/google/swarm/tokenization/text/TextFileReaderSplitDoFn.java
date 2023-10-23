package com.google.swarm.tokenization.text;

import com.google.swarm.tokenization.common.FileReader;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.splittabledofn.OffsetRangeTracker;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.channels.SeekableByteChannel;
import java.util.List;

public class TextFileReaderSplitDoFn extends DoFn<KV<String, FileIO.ReadableFile>, KV<String, String>> {
    public static final Logger LOG = LoggerFactory.getLogger(TextFileReaderSplitDoFn.class);

    private final Integer splitSize;

    private final Integer batchSize;

    public TextFileReaderSplitDoFn(Integer splitSize, Integer batchSize) {
        this.splitSize = splitSize;
        this.batchSize = batchSize;
    }

    @ProcessElement
    public void processElement(ProcessContext c, RestrictionTracker<OffsetRange, Long> tracker) throws IOException {
        String fileName = c.element().getKey();

        try (SeekableByteChannel channel = getReader(c.element().getValue())) {
            FileReader reader =
                    new FileReader(
                            channel, tracker.currentRestriction().getFrom(), recordDelimiter.getBytes());
            while (tracker.tryClaim(reader.getStartOfNextRecord())) {
                long readerPosition = reader.getStartOfNextRecord();
                reader.readNextRecord();
                // Skip CSV header row so that it does not get included in the data being de-identified.
                if (readerPosition == 0) {
                    continue;
                }
                String contents = reader.getCurrent().toStringUtf8();
                String key = fileName;
                c.outputWithTimestamp(KV.of(key, contents), c.timestamp());
            }
        }

    }

    @GetInitialRestriction
    public OffsetRange getInitialRestriction(@Element KV<String, FileIO.ReadableFile> file)
            throws IOException {
        long totalBytes = file.getValue().getMetadata().sizeBytes();
        LOG.info("Initial Restriction range from 1 to: {}", totalBytes);
        return new OffsetRange(0, totalBytes);
    }

    @SplitRestriction
    public void splitRestriction(
            @Element KV<String, FileIO.ReadableFile> file,
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

    private static SeekableByteChannel getReader(FileIO.ReadableFile eventFile) {
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
