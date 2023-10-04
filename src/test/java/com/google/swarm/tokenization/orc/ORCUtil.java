package com.google.swarm.tokenization.orc;

import com.google.privacy.dlp.v2.Table;
import com.google.privacy.dlp.v2.Value;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ORCUtil {

    public final Integer numRecords;

    public final TemporaryFolder tmpFolder;

    public ORCUtil(Integer numRecords, TemporaryFolder tmpFolder) {
        this.numRecords = numRecords;
        this.tmpFolder = tmpFolder;
    }

    public String generateORCFile() throws IOException {
        Path testFilePath = new Path(tmpFolder.newFolder().getAbsolutePath(), "test-sample.orc");
        Configuration conf = new Configuration();

        TypeDescription schema = TypeDescription.fromString("struct<column_name1:int,column_name2:int," +
                                                            "column_name3:string>");

        Writer writer = OrcFile.createWriter(testFilePath, OrcFile.writerOptions(conf).setSchema(schema));

        VectorizedRowBatch batch = schema.createRowBatch();
        LongColumnVector first = (LongColumnVector) batch.cols[0];
        LongColumnVector second = (LongColumnVector) batch.cols[1];
        BytesColumnVector third = (BytesColumnVector) batch.cols[2];

        final int BATCH_SIZE = batch.getMaxSize();

        for(long rowIndex = 0; rowIndex < numRecords; ++rowIndex) {
            int row = batch.size++;

            first.vector[row] = rowIndex;
            second.vector[row] = rowIndex * 3;
            byte[] thirdValue = ("string" + rowIndex).getBytes();
            third.setRef(row, thirdValue, 0, thirdValue.length);

            if (row == BATCH_SIZE - 1) {
                writer.addRowBatch(batch);
                batch.reset();
            }
        }
        if (batch.size != 0) {
            writer.addRowBatch(batch);
            batch.reset();
        }

        writer.close();

        return testFilePath.toString();
    }

    public List<Table.Row> generateTableRows() {
        List<Table.Row> tableRows = new ArrayList<>();

        for (long rowIndex = 0; rowIndex < numRecords; rowIndex++) {
            Value value1 = Value.newBuilder().setIntegerValue(rowIndex).build();
            Value value2 = Value.newBuilder().setIntegerValue(rowIndex * 3).build();
            Value value3 = Value.newBuilder().setStringValue("string" + rowIndex).build();

            Table.Row.Builder tableRowBuilder = Table.Row.newBuilder();
            tableRowBuilder.addValues(value1).addValues(value2).addValues(value3);
            tableRows.add(tableRowBuilder.build());
        }

        return tableRows;
    }
}
