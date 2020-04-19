/*
 * Copyright 2019 Google LLC
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

import com.google.auto.value.AutoValue;
import com.google.cloud.dlp.v2.DlpServiceClient;
import com.google.privacy.dlp.v2.ContentItem;
import com.google.privacy.dlp.v2.InspectContentRequest;
import com.google.privacy.dlp.v2.InspectContentResponse;
import com.google.privacy.dlp.v2.ProjectName;
import java.io.IOException;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@AutoValue
public abstract class DLPTransform
    extends PTransform<PCollection<KV<String, String>>, PCollection<Row>> {
  public static final Logger LOG = LoggerFactory.getLogger(DLPTransform.class);
  private static Integer DLP_PAYLOAD_LIMIT = 524288;

  public abstract String inspectTemplateName();

  public abstract String projectId();

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder setInspectTemplateName(String inspectTemplateName);

    public abstract Builder setProjectId(String projectId);

    public abstract DLPTransform build();
  }

  public static Builder newBuilder() {
    return new AutoValue_DLPTransform.Builder();
  }

  @Override
  public PCollection<Row> expand(PCollection<KV<String, String>> input) {
    return input.apply("DLPInspect", ParDo.of(new InspectData(projectId(), inspectTemplateName())));
  }

  public static class InspectData extends DoFn<KV<String, String>, Row> {
    private String projectId;
    private String inspectTemplateName;
    private InspectContentRequest.Builder requestBuilder;
    private final Counter numberOfBytesInspected =
        Metrics.counter(InspectData.class, "NumberOfBytesInspected");

    public InspectData(String projectId, String inspectTemplateName) {
      this.projectId = projectId;
      this.inspectTemplateName = inspectTemplateName;
    }

    @Setup
    public void setup() {
      this.requestBuilder =
          InspectContentRequest.newBuilder()
              .setParent(ProjectName.of(this.projectId).toString())
              .setInspectTemplateName(this.inspectTemplateName);
    }

    @ProcessElement
    public void processElement(ProcessContext c) throws IOException {
      try (DlpServiceClient dlpServiceClient = DlpServiceClient.create()) {
        String fileName = c.element().getKey();

        if (!c.element().getValue().isEmpty()) {
          ContentItem contentItem =
              ContentItem.newBuilder().setValue(c.element().getValue()).build();
          this.requestBuilder.setItem(contentItem);

          if (this.requestBuilder.build().getSerializedSize() > DLP_PAYLOAD_LIMIT) {
            String errorMessage =
                String.format(
                    "Payload Size %s Exceeded Batch Size %s",
                    this.requestBuilder.build().getSerializedSize(), DLP_PAYLOAD_LIMIT);
            LOG.error(errorMessage);
          } else {
            InspectContentResponse response =
                dlpServiceClient.inspectContent(this.requestBuilder.build());
            String timeStamp = Util.getTimeStamp();
            long bytesInspected = contentItem.getSerializedSize();
            LOG.info("bytes inspected {}", bytesInspected);
            if (response.hasResult()) {
              response
                  .getResult()
                  .getFindingsList()
                  .forEach(
                      finding -> {
                        Row row =
                            Row.withSchema(Util.dlpInspectionSchema)
                                .addValues(
                                    fileName,
                                    bytesInspected,
                                    timeStamp,
                                    finding.getInfoType().getName(),
                                    finding.getLikelihood().name(),
                                    finding.getQuote(),
                                    finding.getLocation().getCodepointRange().getStart(),
                                    finding.getLocation().getCodepointRange().getEnd())
                                .build();
                        LOG.debug("Row {}", row);
                        c.output(row);
                      });
              numberOfBytesInspected.inc(bytesInspected);
            }
          }
        }
      }
    }
  }
}
