package com.google.swarm.tokenization.orc;

import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.values.KV;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.OrcFile;

import java.io.IOException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

public class ORCReaderSplittableDoFn extends DoFn<KV<String, FileIO.ReadableFile>, String> {

    public static final Logger LOG = LoggerFactory.getLogger(ORCReaderSplittableDoFn.class);

    private static final String FS_GS_IMPL_DEFAULT =
            "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem";
    private static final String FS_ABS_GS_IMPL_DEFAULT =
            "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS";
    private static final Integer FS_GS_BLOCK_SZ_DEFAULT = 67108864; // 64 MB

    private final String projectId;

    private final String serviceAccount;

    public ORCReaderSplittableDoFn(String projectId, String serviceAccount) {
        this.projectId = projectId;
        this.serviceAccount = serviceAccount;
    }

    @ProcessElement
    public void processElement(ProcessContext context, RestrictionTracker<OffsetRange, Long> tracker) throws IOException {
        String fileName = context.element().getKey();
        FileIO.ReadableFile readableFile = context.element().getValue();
        String filePath = readableFile.getMetadata().resourceId().toString();
        LOG.info(">> filePath: {}", filePath);

        long start = tracker.currentRestriction().getFrom();
        long end = tracker.currentRestriction().getTo();

        if (tracker.tryClaim(end-1)) {
            LOG.info("Creating configuration to read orc...");
            Configuration conf = new Configuration();
            conf.set("fs.gs.inputstream.fast.fail.on.not.found.enable", "true");
            conf.set("fs.gs.impl", FS_GS_IMPL_DEFAULT);
            conf.set("fs.AbstractFileSystem.gs.impl", FS_ABS_GS_IMPL_DEFAULT);
            conf.set("fs.gs.project.id", projectId);
//            conf.set("fs.gs.system.bucket", "dlp-orc-support-398810-demo-data");
//            conf.set("fs.gs.path.encoding", "uri-path");
//            conf.set("fs.gs.working.dir", "/orc-sample-data/");
//            conf.set("fs.gs.impl.disable.cache", "true");
//            conf.setBoolean("google.cloud.auth.service.account.enable", true);
            conf.setInt("fs.gs.block.size", FS_GS_BLOCK_SZ_DEFAULT);
//            conf.set("google.cloud.auth.service.account.email", serviceAccount);
//            conf.setAllowNullValueProperties(true);
            LOG.info("Done creating configuration to read orc!");

            LOG.info("Initiating reader for orc...");
            Reader reader = OrcFile.createReader(new Path(filePath),
                    OrcFile.readerOptions(conf));
            LOG.info("Done initiating reader for orc...");

            RecordReader rows = reader.rows();
            VectorizedRowBatch batch = reader.getSchema().createRowBatch();
            LOG.info("schema:" + reader.getSchema());
            LOG.info("numCols:" + batch.numCols);
            ColumnVector.Type[] colsMap = new ColumnVector.Type[batch.numCols];

            while (rows.nextBatch(batch)) {
                for (int colIndex=0; colIndex<batch.numCols; colIndex++) {
                    LOG.info("col[{}]: {}", colIndex, batch.cols[colIndex].toString());
                }
//                for (int rowIndex=0; rowIndex<batch.size; rowIndex++) {
//                    if (rowIndex == 0) {
//                        LOG.info("row0: {}", (BytesColumnVector)(batch.cols[0]).toString());
//                    }
//                }
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
            context.outputWithTimestamp("done", Instant.now());
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
