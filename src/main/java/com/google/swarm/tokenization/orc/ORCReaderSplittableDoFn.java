package com.google.swarm.tokenization.orc;

import java.io.IOException;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.values.KV;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
            Configuration conf = new Configuration();
            conf.set("fs.gs.inputstream.fast.fail.on.not.found.enable", "true");
            conf.set("fs.gs.impl", FS_GS_IMPL_DEFAULT);
            conf.set("fs.AbstractFileSystem.gs.impl", FS_ABS_GS_IMPL_DEFAULT);
            conf.set("fs.gs.project.id", projectId);
            // conf.setBoolean("google.cloud.auth.service.account.enable", true);
            conf.setInt("fs.gs.block.size", FS_GS_BLOCK_SZ_DEFAULT);
            // conf.set("fs.gs.auth.type", "");
            // conf.set("fs.defaultFS", "gs://orcdata");

            String serviceAccountId = serviceAccount;
            if (serviceAccountId == null)
                throw new IllegalArgumentException(
                        "Missing service account: "
                                + "Either set environment variable GOOGLE_APPLICATION_CREDENTIALS or "
                                + "application-default via gcloud.");

            // conf.set("google.cloud.auth.service.account.email", serviceAccountId);

//            conf.setAllowNullValueProperties(true);
            Reader reader = OrcFile.createReader(new Path(filePath),
                    OrcFile.readerOptions(conf));

            RecordReader rows = reader.rows();
            VectorizedRowBatch batch = reader.getSchema().createRowBatch();
            LOG.info("schema:" + reader.getSchema());
            LOG.info("numCols:" + batch.numCols);
//            ColumnVector.Type[] colsMap = new ColumnVector.Type[batch.numCols];
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
////        System.out.println("date:" + cols1.vector[r]);
////              String date = new SimpleDateFormat("yyyy-MM-dd").format(new Date(cols1.vector[r]));
////              String value2 = String.valueOf(cols1.vector[r]);
//                    String b = LocalDate.ofEpochDay(cols1.vector[r]).atStartOfDay(ZoneOffset.UTC).format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));
////              System.out.println("date:" + date);
//
//                    Double c = cols2.vector[r];
//                    Timestamp d = cols3.asScratchTimestamp(r);
//                    String e = cols4.toString(r);
//
////              String timeV = new String(insertTime.vector[r], insertTime.start[r], insertTime.length[r]);
////              String value2 = jobId.length[r] == 0 ? "": new String(jobId.vector[r], jobId.start[r], jobId.length[r]);
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
