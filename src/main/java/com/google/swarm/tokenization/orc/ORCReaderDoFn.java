/*
 * Copyright 2023 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.swarm.tokenization.orc;

import com.google.privacy.dlp.v2.Table;
import com.google.privacy.dlp.v2.Value;
import java.io.IOException;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.hadoop.hive.ql.exec.vector.*;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ORCReaderDoFn extends DoFn<KV<String, FileIO.ReadableFile>, KV<String, Table.Row>> {

  public static final Logger LOG = LoggerFactory.getLogger(ORCReaderDoFn.class);

  private final String projectId;

  private VectorizedRowBatch batch;

  public ORCReaderDoFn(String projectId) {
    this.projectId = projectId;
  }

  public Value convertORCFieldToTableValue(int columnIndex, int rowIndex) {
    ColumnVector.Type orcFieldType = batch.cols[columnIndex].type;
    Value tableRowValue;

    if (batch.cols[columnIndex].isNull[rowIndex] == true) {
      tableRowValue = Value.getDefaultInstance();
      return tableRowValue;
    }

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
        StringBuilder bufferDecimal = new StringBuilder();
        DecimalColumnVector decimalColumnVector = (DecimalColumnVector) batch.cols[columnIndex];
        decimalColumnVector.stringifyValue(bufferDecimal, rowIndex);
        tableRowValue = Value.newBuilder().setStringValue(bufferDecimal.toString()).build();
        break;
      case TIMESTAMP:
        /**
         * TimestampColumnVector uses java.sql.Timestamp which represents time in 'seconds' (as
         * number of seconds) and 'nanos' Value uses com.google.protobuf.Timestamp and represents
         * time in 'time' (as number of milliseconds) and 'nanos'
         */
        TimestampColumnVector timestampColumnVector =
            (TimestampColumnVector) batch.cols[columnIndex];
        long orcTimestampMillis = timestampColumnVector.getTime(rowIndex);
        int orcTimestampNanos =
            (int)
                (timestampColumnVector.getNanos(rowIndex)
                    + ((orcTimestampMillis % 1000) * 1000000));
        com.google.protobuf.Timestamp protoTimestamp =
            com.google.protobuf.Timestamp.newBuilder()
                .setSeconds(orcTimestampMillis / 1000)
                .setNanos(orcTimestampNanos)
                .build();
        tableRowValue = Value.newBuilder().setTimestampValue(protoTimestamp).build();
        break;
      case INTERVAL_DAY_TIME:
        IntervalDayTimeColumnVector intervalDayTimeColumnVector =
            (IntervalDayTimeColumnVector) batch.cols[columnIndex];
        long orcIntervalSeconds = intervalDayTimeColumnVector.getTotalSeconds(rowIndex);
        int orcIntervalNanos = (int) (intervalDayTimeColumnVector.getNanos(rowIndex));
        com.google.protobuf.Timestamp protoInterval =
            com.google.protobuf.Timestamp.newBuilder()
                .setSeconds(orcIntervalSeconds)
                .setNanos(orcIntervalNanos)
                .build();
        tableRowValue = Value.newBuilder().setTimestampValue(protoInterval).build();
        break;
      case LIST:
        StringBuilder bufferList = new StringBuilder();
        ListColumnVector listColumnVector = (ListColumnVector) batch.cols[columnIndex];
        listColumnVector.stringifyValue(bufferList, rowIndex);
        tableRowValue = Value.newBuilder().setStringValue(bufferList.toString()).build();
        break;
      case MAP:
        StringBuilder bufferMap = new StringBuilder();
        MapColumnVector mapColumnVector = (MapColumnVector) batch.cols[columnIndex];
        mapColumnVector.stringifyValue(bufferMap, rowIndex);
        tableRowValue = Value.newBuilder().setStringValue(bufferMap.toString()).build();
        break;
      case UNION:
        StringBuilder bufferUnion = new StringBuilder();
        UnionColumnVector unionColumnVector = (UnionColumnVector) batch.cols[columnIndex];
        unionColumnVector.stringifyValue(bufferUnion, rowIndex);
        tableRowValue = Value.newBuilder().setStringValue(bufferUnion.toString()).build();
        break;
      case STRUCT:
        StringBuilder bufferStruct = new StringBuilder();
        StructColumnVector structColumnVector = (StructColumnVector) batch.cols[columnIndex];
        structColumnVector.stringifyValue(bufferStruct, rowIndex);
        tableRowValue = Value.newBuilder().setStringValue(bufferStruct.toString()).build();
        break;
      case BYTES:
      case NONE:
      case VOID:
        String orcBytesValue = ((BytesColumnVector) batch.cols[columnIndex]).toString(rowIndex);
        tableRowValue = Value.newBuilder().setStringValue(orcBytesValue).build();
        break;
      default:
        throw new IllegalArgumentException(
            "Incorrect ColumnVector.type found while reading ORC field value.");
    }

    return tableRowValue;
  }

  @ProcessElement
  public void processElement(ProcessContext context) throws IOException {
    String fileName = context.element().getKey();
    FileIO.ReadableFile readableFile = context.element().getValue();
    String filePath = readableFile.getMetadata().resourceId().toString();

    Reader reader = new ORCFileReader().createORCFileReader(filePath, projectId);

    /**
     * Create a RecordReader to read the ORC records row-by-row. RecordReader can be used to read
     * data in batches and every batch (VectorizedRowBatch) contains the data for 1024 rows.
     */
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
