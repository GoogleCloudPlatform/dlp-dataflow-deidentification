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

import com.google.api.services.bigquery.model.TableCell;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.auto.value.AutoValue;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryOptions;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryUtils;
import org.apache.beam.sdk.io.gcp.bigquery.DynamicDestinations;
import org.apache.beam.sdk.io.gcp.bigquery.TableDestination;
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.ValueInSingleWindow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@AutoValue
@SuppressWarnings("serial")
public abstract class BigQueryDynamicWriteTransform
    extends PTransform<PCollection<KV<String, TableRow>>, WriteResult> {

  public static final Logger LOG = LoggerFactory.getLogger(BigQueryDynamicWriteTransform.class);

  public abstract String projectId();

  public abstract String datasetId();

  public static Builder newBuilder() {
    return new AutoValue_BigQueryDynamicWriteTransform.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setDatasetId(String projectId);

    public abstract Builder setProjectId(String datasetId);

    public abstract BigQueryDynamicWriteTransform build();
  }

  @Override
  public WriteResult expand(PCollection<KV<String, TableRow>> input) {

    Write<KV<String, TableRow>> transform =
        BigQueryIO.<KV<String, TableRow>>write()
            .to(new BQDestination(datasetId(), projectId()))
            .withFormatFunction(KV::getValue)
            .withWriteDisposition(WriteDisposition.WRITE_APPEND)
            .withoutValidation()
            .ignoreInsertIds()
            .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED);

    if (input.getPipeline().getOptions().as(StreamingOptions.class).isStreaming()) {
      transform = transform.withAutoSharding();
    }

    if (!input.getPipeline().getOptions().as(BigQueryOptions.class).getUseStorageWriteApi()) {
      transform = transform.withMethod(Write.Method.STREAMING_INSERTS);
    }

    return input.apply("BQ Write", transform);
  }

  public class BQDestination
      extends DynamicDestinations<KV<String, TableRow>, KV<String, TableRow>> {

    private String datasetName;
    private String projectId;

    public BQDestination(String datasetName, String projectId) {
      this.datasetName = datasetName;
      this.projectId = projectId;
    }

    @Override
    public TableDestination getTable(KV<String, TableRow> destination) {
      TableDestination dest =
          new TableDestination(destination.getKey(), "DLP Transformation Storage Table");
      LOG.debug("Table Destination {}", dest.getTableSpec());
      return dest;
    }

    @Override
    public KV<String, TableRow> getDestination(ValueInSingleWindow<KV<String, TableRow>> element) {
      String key = element.getValue().getKey();
      String tableName = String.format("%s:%s.%s", projectId, datasetName, key);
      LOG.debug("Table Name {}", tableName);
      return KV.of(tableName, element.getValue().getValue());
    }

    @Override
    public TableSchema getSchema(KV<String, TableRow> destination) {
      String tableName = destination.getKey().split("\\.")[1];
      LOG.info("Table Name {}", tableName);
      switch (tableName) {
        case "dlp_inspection_result":
          return BigQueryUtils.toTableSchema(Util.dlpInspectionSchema);
        case "error_table":
          return BigQueryUtils.toTableSchema(Util.errorSchema);
        default:
          TableRow bqRow = destination.getValue();
          TableSchema schema = new TableSchema();
          List<TableFieldSchema> fields = new ArrayList<>();
          List<TableCell> cells = bqRow.getF();
          for (int i = 0; i < cells.size(); i++) {
            Map<String, Object> object = cells.get(i);
            String header =
                object.keySet().stream().filter(name -> !name.equals("v")).findFirst().get();
            /* currently all BQ data types are set to String */
            fields.add(
                new TableFieldSchema().setName(Util.checkHeaderName(header)).setType("STRING"));
          }
          schema.setFields(fields);
          return schema;
      }
    }
  }
}
