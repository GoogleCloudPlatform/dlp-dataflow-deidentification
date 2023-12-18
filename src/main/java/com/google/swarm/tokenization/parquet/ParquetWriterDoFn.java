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
package com.google.swarm.tokenization.parquet;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.privacy.dlp.v2.Table;
import com.google.privacy.dlp.v2.Value;
import java.io.File;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.io.DatumWriter;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ParquetWriterDoFn extends DoFn<KV<String, Iterable<Table.Row>>, KV<String, GenericRecord>> {

  public static Logger LOG = LoggerFactory.getLogger(ParquetWriterDoFn.class);

  private final String outputBucketName;

  private final PCollectionView<Map<String, String>> schemaMapping;

  public ParquetWriterDoFn(
      String outputBucketName, PCollectionView<Map<String, String>> schemaMapping) {
    this.outputBucketName = outputBucketName;
    this.schemaMapping = schemaMapping;
  }

  public Object convertValueToGenericRecord(Object tableRowValue, Schema valueSchema) {
    // if complex schema, make recursive calls and get the value for curr node

    switch (valueSchema.getType()) {
      case RECORD:
        GenericRecordBuilder genericRecordBuilder = new GenericRecordBuilder(valueSchema);
        Gson gson = new Gson();
        JsonObject json = gson.fromJson((String) tableRowValue, JsonObject.class);
        LOG.info("json object is {}, for schema {}", json, valueSchema);
        for (Schema.Field field : valueSchema.getFields()) {
          // extract individual values for field
          // TODO: validate the assumption that RECORD fields are valid JSON objects
          String currentKey = field.name();
          String currentValue = json.get(currentKey).toString();
          Object newValue = convertValueToGenericRecord(currentValue, field.schema());
          genericRecordBuilder.set(field, newValue);
        }
        return genericRecordBuilder.build();

      case STRING:
      default:
        return (String) tableRowValue;
    }
  }

  @ProcessElement
  public void processElement(ProcessContext context) throws IOException {
    String filename = context.element().getKey();
    Iterable<Table.Row> tableRowIterable = context.element().getValue();
    Map<String, String> schemaMappings = context.sideInput(schemaMapping);
    Schema schema = Schema.parse(schemaMappings.get(filename));
    LOG.info("Parquet schema in ParquetWriter: {}", schema);
    String filePath =
        outputBucketName + "/" + filename + "_" + Instant.now().toString() + ".parquet";

    List<GenericRecord> genericRecordList = new ArrayList<>();

    Consumer<Table.Row> assignGenericRowValue =
        (Table.Row currRecord) -> {
          if (currRecord.getValuesCount() != schema.getFields().size()) {
            throw new RuntimeException(
                "Size of Table.Row object ("
                    + currRecord.getValuesCount()
                    + ") mismatched"
                    + " with size of Parquet fieldNames ("
                    + schema.getFields().size()
                    + ").");
          }

          GenericRecordBuilder genericRecordBuilder = new GenericRecordBuilder(schema);
          int i = 0;
          for (Value tableRowValue : currRecord.getValuesList()) {
            Schema.Field currField = schema.getFields().get(i++);
            LOG.info("currField: {}, value: {}, schema: {}", currField, tableRowValue.getStringValue(), currField.schema());
            Object newObject =
                convertValueToGenericRecord(tableRowValue.getStringValue(), currField.schema());
            genericRecordBuilder.set(currField.name(), newObject);
          }

          context.output(KV.of(filename, genericRecordBuilder.build()));
        };

    tableRowIterable.forEach(assignGenericRowValue);
  }
}
