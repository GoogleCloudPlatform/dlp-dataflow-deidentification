/*
 * Copyright 2020 Google LLC
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
package com.google.swarm.tokenization.common;

import com.google.api.gax.rpc.InvalidArgumentException;
import com.google.api.services.bigquery.model.TableReference;
import com.google.auto.value.AutoValue;
import com.google.cloud.bigquery.storage.v1beta1.BigQueryStorageClient;
import com.google.cloud.bigquery.storage.v1beta1.ReadOptions.TableReadOptions;
import com.google.cloud.bigquery.storage.v1beta1.Storage.CreateReadSessionRequest;
import com.google.cloud.bigquery.storage.v1beta1.Storage.ReadSession;
import com.google.cloud.bigquery.storage.v1beta1.TableReferenceProto;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryHelpers;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.TypedRead.Method;
import org.apache.beam.sdk.io.gcp.bigquery.SchemaAndRecord;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@AutoValue
@SuppressWarnings({"deprecation", "serial"})
public abstract class BigQueryReadTransform
    extends PTransform<PBegin, PCollection<KV<String, String>>> {
  public static final Logger LOG = LoggerFactory.getLogger(BigQueryReadTransform.class);

  public abstract String tableRef();

  public abstract Method readMethod();

  public abstract List<String> fields();

  public abstract Integer keyRange();

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setTableRef(String tableRef);

    public abstract Builder setReadMethod(Method readMethod);

    public abstract Builder setFields(List<String> fields);

    public abstract Builder setKeyRange(Integer keyRange);

    public abstract BigQueryReadTransform build();
  }

  public static Builder newBuilder() {
    return new AutoValue_BigQueryReadTransform.Builder();
  }

  @Override
  public PCollection<KV<String, String>> expand(PBegin input) {

    TableReadOptions.Builder builder = TableReadOptions.newBuilder();
    if (fields() != null) {
      builder.addAllSelectedFields(fields());
    }
    TableReadOptions tableReadOptions = builder.build();
    BigQueryStorageClient client = BigQueryStorageClientFactory.create();
    ReadSession session = ReadSessionFactory.create(client, tableRef(), tableReadOptions);

    // Extract schema from ReadSession
    Schema schema = getTableSchema(session);
    client.close();

    return input
        .apply(
            "ReadFromBigQuery",
            BigQueryIO.read(SchemaAndRecord::getRecord)
                .from(tableRef())
                .withTemplateCompatibility()
                .withMethod(readMethod())
                .withCoder(AvroCoder.of(schema))
                .withReadOptions(tableReadOptions))
        .apply("StringConvert", ParDo.of(new ConvertToString(keyRange(), tableRef())));
  }

  static class ConvertToString extends DoFn<GenericRecord, KV<String, String>> {

    private Integer keyRange;
    String tableRef;

    public ConvertToString(Integer keyRange, String tableRef) {
      this.keyRange = keyRange;
      this.tableRef = tableRef;
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
      List<String> values = new ArrayList<>();
      String key = String.format("%s~%d", tableRef, new Random().nextInt(keyRange));
      c.element()
          .getSchema()
          .getFields()
          .forEach(
              field -> {
                values.add(c.element().get(field.name()).toString());
              });

      c.output(KV.of(key, values.stream().collect(Collectors.joining(","))));
    }
  }

  static class BigQueryStorageClientFactory {

    static BigQueryStorageClient create() {
      try {
        return BigQueryStorageClient.create();
      } catch (IOException e) {
        LOG.error("Error connecting to BigQueryStorage API: " + e.getMessage());
        throw new RuntimeException(e);
      }
    }
  }

  private static Schema getTableSchema(ReadSession session) {
    Schema avroSchema;

    avroSchema = new Schema.Parser().parse(session.getAvroSchema().getSchema());
    LOG.info("Schema for export is: " + avroSchema.toString());

    return avroSchema;
  }

  static class ReadSessionFactory {
    static ReadSession create(
        BigQueryStorageClient client, String tableString, TableReadOptions tableReadOptions) {
      TableReference tableReference = BigQueryHelpers.parseTableSpec(tableString);
      String parentProjectId = "projects/" + tableReference.getProjectId();

      TableReferenceProto.TableReference storageTableRef =
          TableReferenceProto.TableReference.newBuilder()
              .setProjectId(tableReference.getProjectId())
              .setDatasetId(tableReference.getDatasetId())
              .setTableId(tableReference.getTableId())
              .build();

      CreateReadSessionRequest.Builder builder =
          CreateReadSessionRequest.newBuilder()
              .setParent(parentProjectId)
              .setReadOptions(tableReadOptions)
              .setTableReference(storageTableRef);
      try {
        return client.createReadSession(builder.build());
      } catch (InvalidArgumentException iae) {
        LOG.error("Error creating ReadSession: " + iae.getMessage());
        throw new RuntimeException(iae);
      }
    }
  }
}
