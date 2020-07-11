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
import com.google.privacy.dlp.v2.InspectContentResponse;
import java.util.List;
import org.apache.beam.sdk.extensions.ml.DLPInspectText;
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

  public abstract String inspectTemplateName();

  public abstract Integer batchSize();

  public abstract String projectId();

  public abstract String columnDelimeter();

  public abstract String dlpmethod();

  public abstract PCollectionView<List<String>> csvHeader();

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder setInspectTemplateName(String inspectTemplateName);

    public abstract Builder setBatchSize(Integer batchSize);

    public abstract Builder setProjectId(String projectId);

    public abstract Builder setCsvHeader(PCollectionView<List<String>> header);

    public abstract Builder setColumnDelimeter(String columnDelimeter);

    public abstract Builder setDlpmethod(String method);

    public abstract DLPTransform build();
  }

  public static Builder newBuilder() {
    return new AutoValue_DLPTransform.Builder();
  }

  @Override
  public PCollection<KV<String, TableRow>> expand(PCollection<KV<String, String>> input) {

    LOG.info("DLP method {}", dlpmethod());

    switch (dlpmethod()) {
      case "inspect":
        {
          return input
              .apply(
                  "DLPInspect",
                  DLPInspectText.newBuilder()
                      .setBatchSizeBytes(batchSize())
                      .setColumnDelimiter(columnDelimeter())
                      .setHeaderColumns(csvHeader())
                      .setInspectTemplateName(inspectTemplateName())
                      .setProjectId(projectId())
                      .build())
              .apply(
                  "CnvertResponse",
                  ParDo.of(new ConvertInspectResponse())
                      .withOutputTags(Util.inspectSuccess, TupleTagList.of(Util.inspectFailure)))
              .get(Util.inspectSuccess);
        }
      case "deid":
        {
          return null;
        }
      case "reid":
        {
          return null;
        }
      default:
        {
          return null;
        }
    }
  }

  static class ConvertInspectResponse
      extends DoFn<KV<String, InspectContentResponse>, KV<String, TableRow>> {
    private final Counter numberOfBytesInspected =
        Metrics.counter(ConvertInspectResponse.class, "NumberOfBytesInspected");

    @ProcessElement
    public void processElement(
        @Element KV<String, InspectContentResponse> element, MultiOutputReceiver out) {
      String fileName = element.getKey();
      String timeStamp = Util.getTimeStamp();
      numberOfBytesInspected.inc(element.getValue().getResult().getSerializedSize());
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
                LOG.info("Row{}", row);
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
