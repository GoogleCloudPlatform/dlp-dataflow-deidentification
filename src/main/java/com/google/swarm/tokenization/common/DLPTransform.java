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
import org.apache.beam.sdk.state.BagState;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.state.Timer;
import org.apache.beam.sdk.state.TimerSpec;
import org.apache.beam.sdk.state.TimerSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@AutoValue
public abstract class DLPTransform
    extends PTransform<PCollection<KV<String, String>>, PCollection<Row>> {
  public static final Logger LOG = LoggerFactory.getLogger(DLPTransform.class);

  public abstract String inspectTemplateName();

  public abstract Integer batchSize();

  public abstract String projectId();

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder setInspectTemplateName(String inspectTemplateName);

    public abstract Builder setBatchSize(Integer batchSize);

    public abstract Builder setProjectId(String projectId);

    public abstract DLPTransform build();
  }

  public static Builder newBuilder() {
    return new AutoValue_DLPTransform.Builder();
  }

  @Override
  public PCollection<Row> expand(PCollection<KV<String, String>> input) {
    return input
        .apply("Batch Contents", ParDo.of(new BatchRequest(batchSize())))
        .apply("DLPInspect", ParDo.of(new InspectData(projectId(), inspectTemplateName())));
  }

  private static KV<String, String> emitResult(Iterable<KV<String, String>> bufferData) {
    StringBuilder builder = new StringBuilder();
    String fileName =
        (bufferData.iterator().hasNext()) ? bufferData.iterator().next().getKey() : "UNKNOWN_FILE";
    bufferData.forEach(
        e -> {
          builder.append(e.getValue());
        });
    return KV.of(fileName, builder.toString());
  }

  private static void clearState(
      BagState<KV<String, String>> elementsBag, ValueState<Integer> elementsSize) {
    elementsBag.clear();
    elementsSize.clear();
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
          if (this.requestBuilder.build().getSerializedSize() > Util.DLP_PAYLOAD_LIMIT) {
            String errorMessage =
                String.format(
                    "Payload Size %s Exceeded Batch Size %s",
                    this.requestBuilder.build().getSerializedSize(), Util.DLP_PAYLOAD_LIMIT);
            LOG.error(errorMessage);
          } else {
            InspectContentResponse response =
                dlpServiceClient.inspectContent(this.requestBuilder.build());
            response
                .getResult()
                .getFindingsList()
                .forEach(
                    finding -> {
                      Row row =
                          Row.withSchema(Util.dlpInspectionSchema)
                              .addValues(
                                  fileName,
                                  Util.getTimeStamp(),
                                  finding.getInfoType().getName(),
                                  finding.getLikelihood().name(),
                                  finding.getLocation().getCodepointRange().getStart(),
                                  finding.getLocation().getCodepointRange().getEnd())
                              .build();
                      LOG.info("Row: {}", row);
                      c.output(row);
                    });
            numberOfBytesInspected.inc(contentItem.getSerializedSize());
          }
        }
      }
    }
  }

  public static class BatchRequest extends DoFn<KV<String, String>, KV<String, String>> {
    private Integer batchSize;

    public BatchRequest(Integer batchSize) {
      this.batchSize = batchSize;
    }

    @StateId("elementsBag")
    private final StateSpec<BagState<KV<String, String>>> elementsBag = StateSpecs.bag();

    @StateId("elementsSize")
    private final StateSpec<ValueState<Integer>> elementsSize = StateSpecs.value();

    @TimerId("eventTimer")
    private final TimerSpec eventTimer = TimerSpecs.timer(TimeDomain.EVENT_TIME);

    @ProcessElement
    public void process(
        @Element KV<String, String> element,
        @StateId("elementsBag") BagState<KV<String, String>> elementsBag,
        @StateId("elementsSize") ValueState<Integer> elementsSize,
        @Timestamp Instant elementTs,
        @TimerId("eventTimer") Timer eventTimer,
        OutputReceiver<KV<String, String>> output) {
      eventTimer.set(elementTs);
      Integer currentElementSize =
          (element.getValue() == null) ? 0 : element.getValue().getBytes().length;
      Integer currentBufferSize = (elementsSize.read() == null) ? 0 : elementsSize.read();
      boolean clearBuffer = (currentElementSize + currentBufferSize) > batchSize;
      if (clearBuffer) {
        KV<String, String> inspectBufferedData = emitResult(elementsBag.read());
        output.output(inspectBufferedData);
        LOG.info(
            "****CLEAR BUFFER Key {} **** Current Content Size {}",
            inspectBufferedData.getKey(),
            inspectBufferedData.getValue().getBytes().length);
        clearState(elementsBag, elementsSize);
      } else {
        elementsBag.add(element);
        elementsSize.write(currentElementSize + currentBufferSize);
      }
    }

    @OnTimer("eventTimer")
    public void onTimer(
        @StateId("elementsBag") BagState<KV<String, String>> elementsBag,
        @StateId("elementsSize") ValueState<Integer> elementsSize,
        OutputReceiver<KV<String, String>> output) {
      // Process left over records less than  batch size
      KV<String, String> inspectBufferedData = emitResult(elementsBag.read());
      output.output(inspectBufferedData);
      LOG.info(
          "****Timer Triggered Key {} **** Current Content Size {}",
          inspectBufferedData.getKey(),
          inspectBufferedData.getValue().getBytes().length);
      clearState(elementsBag, elementsSize);
    }
  }
}
