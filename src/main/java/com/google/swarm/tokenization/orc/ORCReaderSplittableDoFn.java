package com.google.swarm.tokenization.orc;

import com.google.privacy.dlp.v2.Table;
import com.google.privacy.dlp.v2.Value;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.values.KV;
import org.apache.hadoop.hive.ql.exec.vector.*;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class ORCReaderSplittableDoFn extends DoFn<KV<String, FileIO.ReadableFile>, KV<String, Table.Row>> {

    public static final Logger LOG = LoggerFactory.getLogger(ORCReaderSplittableDoFn.class);

    private final String projectId;

    private VectorizedRowBatch batch;

    public ORCReaderSplittableDoFn(String projectId) {
        this.projectId = projectId;
    }

    public Value convertORCFieldToTableValue(Integer columnIndex, Integer rowIndex) {
        ColumnVector.Type orcFieldType = batch.cols[columnIndex].type;
        Value tableRowValue;
        LOG.info("row: {}, col: {}, type: {}", rowIndex, columnIndex, orcFieldType);

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
            case NONE:    // Useful when the type of column vector has not been determined yet.
            case BYTES:
            case TIMESTAMP:
            case INTERVAL_DAY_TIME:
            case STRUCT:
            case LIST:
            case MAP:
            case UNION:
            case VOID:
                String orcBytesValue = ((BytesColumnVector) batch.cols[columnIndex]).toString(rowIndex);
                tableRowValue = Value.newBuilder().setStringValue(orcBytesValue).build();
                break;
            default:
                throw new IllegalArgumentException("Incorrect ColumnVector type found for ORC field value.");
        }
        LOG.info("tableRowValue: {}", tableRowValue);

        return tableRowValue;
    }

    @ProcessElement
    public void processElement(ProcessContext context, RestrictionTracker<OffsetRange, Long> tracker) throws IOException {
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
            LOG.info("schema: {}", reader.getSchema());

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

//            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
//            while (rows.nextBatch(batch)) {
//                BytesColumnVector cols0 = (BytesColumnVector) batch.cols[0];
//                LongColumnVector cols1 = (LongColumnVector) batch.cols[1];
//                DoubleColumnVector cols2 = (DoubleColumnVector) batch.cols[2];
//                TimestampColumnVector cols3 = (TimestampColumnVector) batch.cols[3];
//                BytesColumnVector cols4 = (BytesColumnVector) batch.cols[4];
//
//
//                for(int cols = 0; cols < batch.numCols; cols++) {
//                    LOG.info("args = [" + batch.cols[cols].type + "]");
//                }
//
//                for(int r=0; r < batch.size; r++) {
//                    String a = cols0.toString(r);
//                    System.out.println("date:" + cols1.vector[r]);
//                    String date = new SimpleDateFormat("yyyy-MM-dd").format(new Date(cols1.vector[r]));
//                    String value2 = String.valueOf(cols1.vector[r]);
//                    String b = LocalDate.ofEpochDay(cols1.vector[r]).atStartOfDay(ZoneOffset.UTC).format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));
//                    System.out.println("date:" + date);
//
//                    Double c = cols2.vector[r];
//                    java.sql.Timestamp d = cols3.asScratchTimestamp(r);
//                    String e = cols4.toString(r);
//
//                    String timeV = new String(insertTime.vector[r], insertTime.start[r], insertTime.length[r]);
//                    String value3 = jobId.length[r] == 0 ? "": new String(jobId.vector[r], jobId.start[r], jobId.length[r]);
//                    LOG.info(a + ", " + b + ", " + c + ", " + simpleDateFormat.format(d) + ", " + e);
//                }
//            }
            rows.close();
        }
    }

    @GetInitialRestriction
    public OffsetRange getInitialRestriction(@Element KV<String, FileIO.ReadableFile> element)
            throws IOException {
        long totalBytes = element.getValue().getMetadata().sizeBytes();
        LOG.info("Initial Restriction range from {} to {}", 0, totalBytes);
        return new OffsetRange(0, totalBytes);
    }
}
