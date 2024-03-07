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

import com.google.api.services.bigquery.model.TableRow;
import com.google.auto.value.AutoValue;
import com.google.privacy.dlp.v2.DeidentifyContentResponse;
import com.google.privacy.dlp.v2.InspectContentResponse;
import com.google.privacy.dlp.v2.ReidentifyContentResponse;
import com.google.privacy.dlp.v2.Table;
import com.google.swarm.tokenization.beam.DLPDeidentifyText;
import com.google.swarm.tokenization.beam.DLPInspectText;
import com.google.swarm.tokenization.beam.DLPReidentifyText;
import com.google.swarm.tokenization.common.Util.DLPMethod;
import com.google.swarm.tokenization.common.Util.DataSinkType;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryHelpers;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTagList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@AutoValue
public abstract class DLPTransform
    extends PTransform<PCollection<KV<String, Table.Row>>, PCollectionTuple> {

  public static final Logger LOG = LoggerFactory.getLogger(DLPTransform.class);

  @Nullable
  public abstract String inspectTemplateName();

  @Nullable
  public abstract String deidTemplateName();

  public abstract Integer batchSize();

  public abstract String projectId();

  public abstract Character columnDelimiter();

  public abstract DLPMethod dlpmethod();

  public abstract String jobName();

  public abstract PCollectionView<Map<String, List<String>>> headers();

  public abstract Integer dlpApiRetryCount();

  public abstract Integer initialBackoff();

  public abstract DataSinkType dataSinkType();

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setInspectTemplateName(String inspectTemplateName);

    public abstract Builder setDeidTemplateName(String inspectTemplateName);

    public abstract Builder setBatchSize(Integer batchSize);

    public abstract Builder setProjectId(String projectId);

    public abstract Builder setHeaders(PCollectionView<Map<String, List<String>>> headers);

    public abstract Builder setColumnDelimiter(Character columnDelimiter);

    public abstract Builder setDlpmethod(DLPMethod method);

    public abstract Builder setJobName(String jobName);

    public abstract Builder setDlpApiRetryCount(Integer dlpApiRetryCount);

    public abstract Builder setInitialBackoff(Integer initialBackoff);

    public abstract Builder setDataSinkType(DataSinkType dataSinkType);

    public abstract DLPTransform build();
  }

  public static Builder newBuilder() {
    return new AutoValue_DLPTransform.Builder();
  }

  @Override
  public PCollectionTuple expand(PCollection<KV<String, Table.Row>> input) {
    switch (dlpmethod()) {
      case INSPECT:
        {
          return input
              .apply(
                  "InspectTransform",
                  DLPInspectText.newBuilder()
                      .setBatchSizeBytes(batchSize())
                      .setColumnDelimiter(columnDelimiter())
                      .setHeaderColumns(headers())
                      .setInspectTemplateName(inspectTemplateName())
                      .setProjectId(projectId())
                      .setDlpApiRetryCount(dlpApiRetryCount())
                      .setInitialBackoff(initialBackoff())
                      .build())
              .apply(
                  "ConvertInspectResponse",
                  ParDo.of(new ConvertInspectResponse(jobName()))
                      .withOutputTags(
                          Util.inspectOrDeidSuccess, TupleTagList.of(Util.inspectOrDeidFailure)));
        }

      case DEID:
        {
          return input
              .apply(
                  "DeIdTransform",
                  DLPDeidentifyText.newBuilder()
                      .setBatchSizeBytes(batchSize())
                      .setColumnDelimiter(columnDelimiter())
                      .setHeaderColumns(headers())
                      .setInspectTemplateName(inspectTemplateName())
                      .setDeidentifyTemplateName(deidTemplateName())
                      .setProjectId(projectId())
                      .setDlpApiRetryCount(dlpApiRetryCount())
                      .setInitialBackoff(initialBackoff())
                      .build())
              .apply(
                  "ConvertDeidResponse",
                  ParDo.of(new ConvertDeidResponse(dataSinkType()))
                      .withOutputTags(
                          Util.inspectOrDeidSuccess,
                          TupleTagList.of(Util.inspectOrDeidFailure).and(Util.deidSuccessGCS)));
        }
      case REID:
        {
          return input
              .apply(
                  "ReIdTransform",
                  DLPReidentifyText.newBuilder()
                      .setBatchSizeBytes(batchSize())
                      .setColumnDelimiter(columnDelimiter())
                      .setHeaderColumns(headers())
                      .setInspectTemplateName(inspectTemplateName())
                      .setReidentifyTemplateName(deidTemplateName())
                      .setProjectId(projectId())
                      .setDlpApiRetryCount(dlpApiRetryCount())
                      .setInitialBackoff(initialBackoff())
                      .build())
              .apply(
                  "ConvertReidResponse",
                  ParDo.of(new ConvertReidResponse())
                      .withOutputTags(Util.reidSuccess, TupleTagList.of(Util.reidFailure)));
        }
      default:
        {
          throw new IllegalArgumentException("Please validate DLPMethod param!");
        }
    }
  }

  static class ConvertReidResponse
      extends DoFn<KV<String, ReidentifyContentResponse>, KV<String, TableRow>> {

    private final Counter numberOfBytesReidentified =
        Metrics.counter(ConvertDeidResponse.class, "NumberOfBytesReidentified");

    private final Counter numberOfRowsReidentified =
        Metrics.counter(ConvertDeidResponse.class, "numberOfRowsReidentified");

    @ProcessElement
    public void processElement(
        @Element KV<String, ReidentifyContentResponse> element, MultiOutputReceiver out) {

      String deidTableName = BigQueryHelpers.parseTableSpec(element.getKey()).getTableId();
      String tableName = String.format("%s_%s", deidTableName, Util.BQ_REID_TABLE_EXT);
      LOG.info("Table Ref {}", tableName);
      Table originalData = element.getValue().getItem().getTable();
      numberOfBytesReidentified.inc(originalData.toByteArray().length);
      numberOfRowsReidentified.inc(originalData.getRowsCount());
      List<String> headers =
          originalData.getHeadersList().stream()
              .map(fid -> fid.getName())
              .collect(Collectors.toList());
      List<Table.Row> outputRows = originalData.getRowsList();
      if (outputRows.size() > 0) {
        for (Table.Row outputRow : outputRows) {
          if (outputRow.getValuesCount() != headers.size()) {
            throw new IllegalArgumentException(
                "BigQuery column count must exactly match with data element count");
          }
          out.get(Util.reidSuccess)
              .output(
                  KV.of(
                      tableName,
                      Util.createBqRow(outputRow, headers.toArray(new String[headers.size()]))));
        }
      }
    }
  }

  static class ConvertDeidResponse
      extends DoFn<KV<String, DeidentifyContentResponse>, KV<String, TableRow>> {
    private DataSinkType dataSink;
    private final Counter numberOfRowDeidentified =
        Metrics.counter(ConvertDeidResponse.class, "numberOfRowDeidentified");

    public ConvertDeidResponse(DataSinkType dataSink) {
      this.dataSink = dataSink;
    }

    @ProcessElement
    public void processElement(
        @Element KV<String, DeidentifyContentResponse> element, MultiOutputReceiver out) {

      String fileName = element.getKey();
      Table tokenizedData = element.getValue().getItem().getTable();
      LOG.info("Table de-identified returned with {} rows", tokenizedData.getRowsCount());
      numberOfRowDeidentified.inc(tokenizedData.getRowsCount());
      List<String> headers =
          tokenizedData.getHeadersList().stream()
              .map(fid -> fid.getName())
              .collect(Collectors.toList());
      List<Table.Row> outputRows = tokenizedData.getRowsList();
      if (outputRows.size() > 0) {
        for (Table.Row outputRow : outputRows) {
          if (outputRow.getValuesCount() != headers.size()) {
            throw new IllegalArgumentException(
                "CSV file's header count must exactly match with data element count");
          }
          if (this.dataSink == DataSinkType.BigQuery) {
            out.get(Util.inspectOrDeidSuccess)
                .output(
                    KV.of(
                        fileName,
                        Util.createBqRow(outputRow, headers.toArray(new String[headers.size()]))));
          } else if (this.dataSink == DataSinkType.GCS) {
            out.get(Util.deidSuccessGCS).output(KV.of(fileName, outputRow));
          }
        }
      }
    }
  }

  static class ConvertInspectResponse
      extends DoFn<KV<String, InspectContentResponse>, KV<String, TableRow>> {

    private String jobName;

    public ConvertInspectResponse(String jobName) {
      this.jobName = jobName;
    }

    // Counter to track total number of Inspection Findings fetched from DLP Inspection response
    private final Counter numberOfInspectionFindings =
        Metrics.counter(ConvertInspectResponse.class, "numberOfInspectionFindings");

    // Counter to track total number of times Inspection Findings got truncated in the
    // in the DLP Inspection response
    private final Counter numberOfTimesFindingsTruncated =
        Metrics.counter(ConvertInspectResponse.class, "numberOfTimesFindingsTruncated");

    // Counter to track total number of times Inspection Findings generated in the
    // this should be same number as number of total DLP API calls
    private final Counter numberOfTimesFindingsGenerated =
        Metrics.counter(ConvertInspectResponse.class, "numberOfTimesFindingsGenerated");

    @ProcessElement
    public void processElement(
        @Element KV<String, InspectContentResponse> element, MultiOutputReceiver out) {
      String fileName = element.getKey().split("\\~")[0];
      String timeStamp = Util.getTimeStamp();

      if (element.getValue().getResult().getFindingsTruncated()) {
        numberOfTimesFindingsTruncated.inc();
      }

      numberOfTimesFindingsGenerated.inc();

      element
          .getValue()
          .getResult()
          .getFindingsList()
          .forEach(
              finding -> {
                Row row =
                    Row.withSchema(Util.dlpInspectionSchema)
                        .addValues(
                            jobName,
                            fileName,
                            timeStamp,
                            finding.getQuote(),
                            finding.getInfoType().getName(),
                            finding.getLikelihood().name(),
                            finding.getLocation().getCodepointRange().getStart(),
                            finding.getLocation().getCodepointRange().getEnd(),
                            finding
                                .getLocation()
                                .getContentLocationsList()
                                .get(0)
                                .getRecordLocation()
                                .getFieldId()
                                .getName())
                        .build();
                numberOfInspectionFindings.inc();
                out.get(Util.inspectOrDeidSuccess)
                    .output(KV.of(Util.BQ_DLP_INSPECT_TABLE_NAME, Util.toTableRow(row)));
              });
      element
          .getValue()
          .findInitializationErrors()
          .forEach(
              error -> {
                out.get(Util.inspectOrDeidFailure)
                    .output(
                        KV.of(
                            Util.BQ_ERROR_TABLE_NAME,
                            Util.toTableRow(
                                Row.withSchema(Util.errorSchema)
                                    .addValues(fileName, timeStamp, error.toString(), null)
                                    .build())));
              });
    }
  }
}
