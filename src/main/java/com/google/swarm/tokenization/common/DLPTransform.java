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
import com.google.gson.JsonSyntaxException;
import com.google.privacy.dlp.v2.DeidentifyContentResponse;
import com.google.privacy.dlp.v2.InspectContentResponse;
import com.google.privacy.dlp.v2.ReidentifyContentResponse;
import com.google.privacy.dlp.v2.Table;
import com.google.protobuf.InvalidProtocolBufferException;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.beam.repackaged.core.org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.Element;
import org.apache.beam.sdk.transforms.DoFn.MultiOutputReceiver;
import org.apache.beam.sdk.transforms.DoFn.ProcessElement;
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

@SuppressWarnings("serial")
@AutoValue
public abstract class DLPTransform
    extends PTransform<PCollection<KV<String, String>>, PCollectionTuple> {
  public static final Logger LOG = LoggerFactory.getLogger(DLPTransform.class);

  @Nullable
  public abstract String inspectTemplateName();

  @Nullable
  public abstract String deidTemplateName();

  public abstract Integer batchSize();

  public abstract String projectId();

  public abstract String columnDelimeter();

  public abstract String dlpmethod();

  public abstract PCollectionView<List<String>> header();

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder setInspectTemplateName(String inspectTemplateName);

    public abstract Builder setDeidTemplateName(String inspectTemplateName);

    public abstract Builder setBatchSize(Integer batchSize);

    public abstract Builder setProjectId(String projectId);

    public abstract Builder setHeader(PCollectionView<List<String>> header);

    public abstract Builder setColumnDelimeter(String columnDelimeter);

    public abstract Builder setDlpmethod(String method);

    public abstract DLPTransform build();
  }

  public static Builder newBuilder() {
    return new AutoValue_DLPTransform.Builder();
  }

  @Override
  public PCollectionTuple expand(PCollection<KV<String, String>> input) {
    PCollection<KV<String, Iterable<Table.Row>>> batchedRows =
        input
            .apply("DLPRowConverts", ParDo.of(new MapStringToDlpRow(columnDelimeter())))
            .apply("BatchContents", ParDo.of(new BatchRequestForDLP(batchSize())));
    switch (dlpmethod()) {
      case "inspect":
        {
          return batchedRows
              .apply(
                  "DLPInspect",
                  ParDo.of(new InspectData(projectId(), inspectTemplateName(), header()))
                      .withSideInputs(header())
                      .withOutputTags(
                          Util.inspectApiCallSuccess, TupleTagList.of(Util.inspectApiCallError)))
              .get(Util.inspectApiCallSuccess)
              .apply(
                  "CnvertInspectResponse",
                  ParDo.of(new ConvertInspectResponse())
                      .withOutputTags(
                          Util.inspectOrDeidSuccess, TupleTagList.of(Util.inspectOrDeidFailure)));
        }
      case "deid":
        {
          return batchedRows
              .apply(
                  "DLPDeidentify",
                  ParDo.of(
                          new DeidentifyData(
                              projectId(), inspectTemplateName(), deidTemplateName(), header()))
                      .withSideInputs(header()))
              .apply(
                  "ConvertDeidResponse",
                  ParDo.of(new ConvertDeidResponse())
                      .withOutputTags(
                          Util.inspectOrDeidSuccess, TupleTagList.of(Util.inspectOrDeidFailure)));
        }
      case "reid":
        {
          return batchedRows
              .apply(
                  "DLPReidentify",
                  ParDo.of(
                          new ReidentifyData(
                              projectId(), inspectTemplateName(), deidTemplateName(), header()))
                      .withSideInputs(header()))
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
      extends DoFn<KV<String, ReidentifyContentResponse>, PubsubMessage> {
    private final Counter numberOfBytesReidentified =
        Metrics.counter(ConvertDeidResponse.class, "NumberOfBytesReidentified");

    @ProcessElement
    public void processElement(
        @Element KV<String, ReidentifyContentResponse> element, MultiOutputReceiver out) {

      String tableRef = element.getKey().split("\\~")[0];
      Table originalData = element.getValue().getItem().getTable();
      numberOfBytesReidentified.inc(originalData.toByteArray().length);
      try {
        out.get(Util.reidSuccess).output(Util.convertPubSubMessage(element.getValue()));
      } catch (JsonSyntaxException | InvalidProtocolBufferException e) {
        out.get(Util.reidFailure)
            .output(
                KV.of(
                    Util.BQ_ERROR_TABLE_NAME,
                    Util.toTableRow(
                        Row.withSchema(Util.errorSchema)
                            .addValues(
                                tableRef,
                                Util.getTimeStamp(),
                                e.toString(),
                                ExceptionUtils.getStackTrace(e))
                            .build())));
      }
    }
  }

  static class ConvertDeidResponse
      extends DoFn<KV<String, DeidentifyContentResponse>, KV<String, TableRow>> {
    private final Counter numberOfBytesDeidentified =
        Metrics.counter(ConvertDeidResponse.class, "NumberOfBytesDeidentified");

    @ProcessElement
    public void processElement(
        @Element KV<String, DeidentifyContentResponse> element, MultiOutputReceiver out) {

      String fileName = element.getKey().split("\\~")[0];
      Table tokenizedData = element.getValue().getItem().getTable();
      numberOfBytesDeidentified.inc(tokenizedData.toByteArray().length);
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
          out.get(Util.inspectOrDeidSuccess)
              .output(
                  KV.of(
                      fileName,
                      Util.createBqRow(outputRow, headers.toArray(new String[headers.size()]))));
        }
      }
    }
  }

  static class ConvertInspectResponse
      extends DoFn<KV<String, InspectContentResponse>, KV<String, TableRow>> {
    private final Counter numberOfInspectionFindings =
        Metrics.counter(ConvertInspectResponse.class, "NumberOfInspectionFindings");

    @ProcessElement
    public void processElement(
        @Element KV<String, InspectContentResponse> element, MultiOutputReceiver out) {
      String fileName = element.getKey().split("\\~")[0];
      String timeStamp = Util.getTimeStamp();
      element
          .getValue()
          .getResult()
          .getFindingsList()
          .forEach(
              finding -> {
                Row row =
                    Row.withSchema(Util.dlpInspectionSchema)
                        .addValues(
                            fileName,
                            timeStamp,
                            finding.getInfoType().getName(),
                            finding.getLikelihood().name(),
                            finding.getQuote(),
                            finding.getLocation().getCodepointRange().getStart(),
                            finding.getLocation().getCodepointRange().getEnd())
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
