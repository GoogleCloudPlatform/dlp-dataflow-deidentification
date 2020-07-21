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
import com.google.api.services.bigquery.model.TableRow;
import com.google.auto.value.AutoValue;
import com.google.privacy.dlp.v2.DeidentifyContentResponse;
import com.google.privacy.dlp.v2.InspectContentResponse;
import com.google.privacy.dlp.v2.Table;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.MultiOutputReceiver;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTagList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("serial")
@AutoValue
public abstract class DLPTransform
    extends PTransform<PCollection<KV<String, String>>, PCollection<KV<String, TableRow>>> {
  public static final Logger LOG = LoggerFactory.getLogger(DLPTransform.class);

  @Nullable
  public abstract String inspectTemplateName();

  @Nullable
  public abstract String deidTemplateName();

  public abstract Integer batchSize();

  public abstract String projectId();

  public abstract String columnDelimeter();

  public abstract String dlpmethod();

  public abstract String jobName();

  public abstract PCollectionView<List<String>> csvHeader();

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder setInspectTemplateName(String inspectTemplateName);

    public abstract Builder setDeidTemplateName(String inspectTemplateName);

    public abstract Builder setBatchSize(Integer batchSize);

    public abstract Builder setProjectId(String projectId);

    public abstract Builder setCsvHeader(PCollectionView<List<String>> header);

    public abstract Builder setColumnDelimeter(String columnDelimeter);

    public abstract Builder setDlpmethod(String method);

    public abstract Builder setJobName(String jobName);

    public abstract DLPTransform build();
  }

  public static Builder newBuilder() {
    return new AutoValue_DLPTransform.Builder();
  }

  @Override
  public PCollection<KV<String, TableRow>> expand(PCollection<KV<String, String>> input) {
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
                  ParDo.of(new InspectData(projectId(), inspectTemplateName(), csvHeader()))
                      .withSideInputs(csvHeader())
                      .withOutputTags(
                          Util.inspectApiCallSuccess, TupleTagList.of(Util.inspectApiCallError)))
              .get(Util.inspectApiCallSuccess)
              .apply(
                  "ConvertInspectResponse",
                  ParDo.of(new ConvertInspectResponse(jobName()))
                      .withOutputTags(Util.inspectSuccess, TupleTagList.of(Util.inspectFailure)))
              .get(Util.inspectSuccess);
        }
      case "deid":
        {
          return batchedRows
              .apply(
                  "DLPDeidentify",
                  ParDo.of(
                          new DeidentifyData(
                              projectId(), inspectTemplateName(), deidTemplateName(), csvHeader()))
                      .withSideInputs(csvHeader()))
              .apply(
                  "ConvertDeidResponse",
                  ParDo.of(new ConvertDeidResponse())
                      .withOutputTags(Util.deidSuccess, TupleTagList.of(Util.deidFailure)))
              .get(Util.deidSuccess);
        }
      case "reid":
        {
          throw new IllegalArgumentException("Not Supported yet");
        }
      default:
        {
          throw new IllegalArgumentException("Please double check DLP Method Param!");
        }
    }
  }

  static class ConvertDeidResponse
      extends DoFn<KV<String, DeidentifyContentResponse>, KV<String, TableRow>> {
    private final Counter numberOfBytesDeidentified =
        Metrics.counter(ConvertInspectResponse.class, "NumberOfBytesDeidentified");

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
          out.get(Util.deidSuccess)
              .output(
                  KV.of(
                      fileName,
                      createBqRow(outputRow, headers.toArray(new String[headers.size()]))));
        }
      }
    }

    private static TableRow createBqRow(Table.Row tokenizedValue, String[] headers) {
      TableRow bqRow = new TableRow();
      AtomicInteger headerIndex = new AtomicInteger(0);
      List<TableCell> cells = new ArrayList<>();
      tokenizedValue
          .getValuesList()
          .forEach(
              value -> {
                String checkedHeaderName =
                    Util.checkHeaderName(headers[headerIndex.getAndIncrement()].toString());
                bqRow.set(checkedHeaderName, value.getStringValue());
                cells.add(new TableCell().set(checkedHeaderName, value.getStringValue()));
              });
      bqRow.setF(cells);
      return bqRow;
    }
  }

  static class ConvertInspectResponse
      extends DoFn<KV<String, InspectContentResponse>, KV<String, TableRow>> {

    private String jobName;

    public ConvertInspectResponse(String jobName) {
      this.jobName = jobName;
    }

    private final Counter numberOfInspectionFindings =
        Metrics.counter(ConvertInspectResponse.class, "NumberOfInspectionFindings");

    private final Counter numberOfTimesFindingsTruncated =
        Metrics.counter(ConvertInspectResponse.class, "NumberOfTimesFindingsTruncated");

    private final Counter numberOfTimesFindingsGenerated =
        Metrics.counter(ConvertInspectResponse.class, "NumberOfTimesFindingsGenerated");

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
                LOG.debug("Row{}", row);
                numberOfInspectionFindings.inc();
                out.get(Util.inspectSuccess)
                    .output(KV.of(Util.BQ_DLP_INSPECT_TABLE_NAME, Util.toTableRow(row)));
              });
      element
          .getValue()
          .findInitializationErrors()
          .forEach(
              error -> {
                out.get(Util.inspectFailure)
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
