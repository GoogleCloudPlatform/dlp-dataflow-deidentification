package com.google.swarm.tokenization.orc;

import com.google.privacy.dlp.v2.Table;
import com.google.privacy.dlp.v2.Value;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.splittabledofn.OffsetRangeTracker;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.values.KV;
import org.apache.hadoop.hive.ql.exec.vector.*;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

public class ORCReaderSplittableDoFn extends DoFn<KV<String, FileIO.ReadableFile>, KV<String, Table.Row>> {

    public static final Logger LOG = LoggerFactory.getLogger(ORCReaderSplittableDoFn.class);

    private final String projectId;

    private final Integer splitSize;

    private VectorizedRowBatch batch;

    public ORCReaderSplittableDoFn(String projectId, Integer splitSize) {
        this.projectId = projectId;
        this.splitSize = splitSize;
    }

    public Value convertORCFieldToTableValue(Integer columnIndex, Integer rowIndex) {
        ColumnVector.Type orcFieldType = batch.cols[columnIndex].type;
        Value tableRowValue;

        switch (orcFieldType) {
            case LONG:
                long orcLongValue = ((LongColumnVector) batch.cols[columnIndex]).vector[rowIndex];
                tableRowValue = Value.newBuilder().setIntegerValue(orcLongValue).build();
                break;
            case DOUBLE:
                double orcDoubleValue = ((DoubleColumnVector) batch.cols[columnIndex]).vector[rowIndex];
                tableRowValue = Value.newBuilder().setFloatValue(orcDoubleValue).build();
                break;
            case DECIMAL:
            case DECIMAL_64:
                String orcDecimalValue = ((DecimalColumnVector) batch.cols[columnIndex]).vector[rowIndex].toString();
                tableRowValue = Value.newBuilder().setStringValue(orcDecimalValue).build();
                break;
            case TIMESTAMP:
                String orcTimestampValue = ((TimestampColumnVector) batch.cols[columnIndex]).asScratchTimestamp(rowIndex).toString();
                tableRowValue = Value.newBuilder().setStringValue(orcTimestampValue).build();
                break;
            case INTERVAL_DAY_TIME:
                String orcIntervalValue = ((IntervalDayTimeColumnVector) batch.cols[columnIndex]).asScratchIntervalDayTime(rowIndex).toString();
                tableRowValue = Value.newBuilder().setStringValue(orcIntervalValue).build();
                break;
            case NONE:    // Useful when the type of column vector has not been determined yet.
            case BYTES:
            case VOID:
                String orcBytesValue = ((BytesColumnVector) batch.cols[columnIndex]).toString(rowIndex);
                tableRowValue = Value.newBuilder().setStringValue(orcBytesValue).build();
                break;
            case LIST:
            case MAP:
            case UNION:
            case STRUCT:
            default:
                throw new IllegalArgumentException("Incorrect ColumnVector type found for ORC field value.");
        }

        return tableRowValue;
    }

    @ProcessElement
    public void processElement(ProcessContext context, RestrictionTracker<OffsetRange, Long> tracker)
            throws IOException {
        String fileName = context.element().getKey();
        FileIO.ReadableFile readableFile = context.element().getValue();
        String filePath = readableFile.getMetadata().resourceId().toString();

        long start = tracker.currentRestriction().getFrom();
        long end = tracker.currentRestriction().getTo();

        if (tracker.tryClaim(end-1)) {
            Reader reader = new ORCFileReader().createORCFileReader(filePath, projectId);

            RecordReader rows = reader.rows();
            batch = reader.getSchema().createRowBatch();
            ColumnVector.Type[] colsMap = new ColumnVector.Type[batch.numCols];

            while (rows.nextBatch(batch)) {
                for (int rowIndex = 0; rowIndex < batch.size; rowIndex++) {
                    Table.Row.Builder tableRowBuilder = Table.Row.newBuilder();
                    for (int colIndex = 0; colIndex < batch.numCols; colIndex++) {
                        Value convertedValue = convertORCFieldToTableValue(colIndex, rowIndex);
                        tableRowBuilder.addValues(convertedValue);
                    }
                    context.outputWithTimestamp(KV.of(fileName, tableRowBuilder.build()), Instant.now());
                }
            }
            rows.close();
        }
    }

    @GetInitialRestriction
    public OffsetRange getInitialRestriction(@Element KV<String, FileIO.ReadableFile> element) throws IOException {
        long totalBytes = element.getValue().getMetadata().sizeBytes();
        LOG.info("Initial Restriction range from {} to {}", 0, totalBytes);
        return new OffsetRange(0, totalBytes);
    }

//    @SplitRestriction
//    public void splitRestriction(@Element KV<String, FileIO.ReadableFile> file,
//                                 @Restriction OffsetRange range,
//                                 OutputReceiver<OffsetRange> out) {
//        List<OffsetRange> splits = range.split(splitSize, splitSize);
//        LOG.info("Number of splits: {}", splits.size());
//        for (final OffsetRange offsetRange:splits) {
//            out.output(offsetRange);
//        }
//    }
//
//    @NewTracker
//    public OffsetRangeTracker newTracker(@Restriction OffsetRange range) {
//        return new OffsetRangeTracker(new OffsetRange(range.getFrom(), range.getTo()));
//    }
}
