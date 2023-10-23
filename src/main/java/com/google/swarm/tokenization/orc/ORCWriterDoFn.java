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
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.type.HiveIntervalDayTime;
import org.apache.hadoop.hive.ql.exec.vector.*;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ORCWriterDoFn extends DoFn<KV<String, Iterable<Table.Row>>, String> {

  public static Logger LOG = LoggerFactory.getLogger(ORCWriterDoFn.class);

  private final String outputBucketName;

  private final PCollectionView<Map<String, String>> schemaMapping;

  public ORCWriterDoFn(
      String outputBucketName, PCollectionView<Map<String, String>> schemaMapping) {
    this.outputBucketName = outputBucketName;
    this.schemaMapping = schemaMapping;
  }

  public void createORCColumnVectors(VectorizedRowBatch batch, Table.Row tableRow, int rowIndex) {

    for (int columnIndex = 0; columnIndex < tableRow.getValuesCount(); columnIndex++) {
      ColumnVector columnVector = batch.cols[columnIndex];
      Value tableRowValue = tableRow.getValues(columnIndex);

      if (tableRowValue.equals(Value.getDefaultInstance())) {
        columnVector.isNull[rowIndex] = true;
        columnVector.noNulls = false;
      }

      switch (columnVector.type) {
        case LONG:
          LongColumnVector longColumnVector = (LongColumnVector) columnVector;
          long orcLongValue = tableRowValue.getIntegerValue();
          longColumnVector.vector[rowIndex] = orcLongValue;
          break;
        case DOUBLE:
          DoubleColumnVector doubleColumnVector = (DoubleColumnVector) columnVector;
          double orcDoubleValue = tableRowValue.getFloatValue();
          doubleColumnVector.vector[rowIndex] = orcDoubleValue;
          break;
        case DECIMAL:
        case DECIMAL_64:
          DecimalColumnVector decimalColumnVector = (DecimalColumnVector) columnVector;
          String stringyfiedDecimal = tableRowValue.getStringValue();
          HiveDecimalWritable hiveDecimalWritable = new HiveDecimalWritable(stringyfiedDecimal);
          decimalColumnVector.set(rowIndex, hiveDecimalWritable);
          break;
        case TIMESTAMP:
          TimestampColumnVector timestampColumnVector = (TimestampColumnVector) columnVector;
          com.google.protobuf.Timestamp protoTimestamp = tableRowValue.getTimestampValue();
          java.sql.Timestamp timestamp = new java.sql.Timestamp(protoTimestamp.getSeconds());
          timestamp.setNanos(protoTimestamp.getNanos());
          timestampColumnVector.set(rowIndex, timestamp);
          break;
        case INTERVAL_DAY_TIME:
          IntervalDayTimeColumnVector intervalDayTimeColumnVector =
              (IntervalDayTimeColumnVector) columnVector;
          com.google.protobuf.Timestamp protoInterval = tableRowValue.getTimestampValue();
          HiveIntervalDayTime hiveIntervalDayTime =
              new HiveIntervalDayTime(protoInterval.getSeconds(), protoInterval.getNanos());
          intervalDayTimeColumnVector.set(rowIndex, hiveIntervalDayTime);
          break;
        case BYTES:
        case NONE:
        case VOID:
          BytesColumnVector bytesColumnVector = (BytesColumnVector) columnVector;
          String orcStringValue = tableRowValue.getStringValue();
          byte[] orcBytesValue = orcStringValue.getBytes();
          bytesColumnVector.setRef(rowIndex, orcBytesValue, 0, orcBytesValue.length);
          break;
        case LIST:
        case MAP:
        case UNION:
        case STRUCT:
          throw new IllegalArgumentException(
              "Compound ORC data types are not supported to write output in Cloud Storage buckets.");
        default:
          throw new IllegalArgumentException(
              "Incorrect ColumnVector.type found while type casting ColumnVectors.");
      }
    }
  }

  @ProcessElement
  public void processElement(ProcessContext context) throws IOException {
    String filename = context.element().getKey();
    Iterable<Table.Row> tableRowIterable = context.element().getValue();
    Map<String, String> schemaMappings = context.sideInput(schemaMapping);
    TypeDescription schema = TypeDescription.fromString(schemaMappings.get(filename));
    String filePath = outputBucketName + "/" + filename + "_" + Instant.now().toString() + ".orc";

    Configuration conf = new Configuration();
    Path gURIPath = new Path(filePath);
    Writer writer = OrcFile.createWriter(gURIPath, OrcFile.writerOptions(conf).setSchema(schema));

    VectorizedRowBatch batch = schema.createRowBatch();

    List<String> fieldNames = schema.getFieldNames();

    final int BATCH_SIZE = batch.getMaxSize();

    Consumer<Table.Row> assignORCRowValue =
        (Table.Row currRecord) -> {
          int row = batch.size++;

          if (currRecord.getValuesCount() != batch.numCols) {
            throw new RuntimeException(
                "Size of Table.Row object ("
                    + currRecord.getValuesCount()
                    + ") mismatched"
                    + " with size of ORC fieldNames ("
                    + fieldNames.size()
                    + ").");
          }

          createORCColumnVectors(batch, currRecord, row);

          if (row == BATCH_SIZE - 1) {
            try {
              writer.addRowBatch(batch);
            } catch (IOException e) {
              throw new RuntimeException(e);
            }
            batch.reset();
          }
        };

    tableRowIterable.forEach(assignORCRowValue);

    if (batch.size != 0) {
      writer.addRowBatch(batch);
      batch.reset();
    }

    writer.close();

    context.output(filename);
  }
}
