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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.state.BagState;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.state.Timer;
import org.apache.beam.sdk.state.TimerSpec;
import org.apache.beam.sdk.state.TimerSpecs;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTagList;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@AutoValue
public abstract class DLPTransform
    extends PTransform<PCollection<KV<String, String>>, PCollectionTuple> {
  public static final Logger LOG = LoggerFactory.getLogger(DLPTransform.class);

  private static Integer DLP_PAYLOAD_LIMIT = 524288;

  public abstract String inspectTemplateName();

  public abstract String projectId();

  public abstract Integer batchSize();

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
  public PCollectionTuple expand(PCollection<KV<String, String>> input) {
    return input
        .apply("Batch Request", ParDo.of(new BatchRequest(batchSize())))
        .apply(
            "DLPInspect",
            ParDo.of(new InspectData(projectId(), inspectTemplateName()))
                .withOutputTags(Util.inspectData, TupleTagList.of(Util.auditData)));
  }

  public static class BatchRequest extends DoFn<KV<String, String>, KV<String, Iterable<String>>> {

    private static final long serialVersionUID = 1L;
//    private final Counter numberOfRowsBagged =
//        Metrics.counter(BatchRequest.class, "numberOfRowsBagged");
    private Integer batchSize;

    public BatchRequest(Integer batchSize) {
      this.batchSize = batchSize;
    }

    @StateId("elementsBag")
    private final StateSpec<BagState<KV<String, String>>> elementsBag = StateSpecs.bag();

    @TimerId("eventTimer")
    private final TimerSpec eventTimer = TimerSpecs.timer(TimeDomain.PROCESSING_TIME);

    @ProcessElement
    public void process(
        @Element KV<String, String> element,
        @TimerId("eventTimer") Timer eventTimer,
        BoundedWindow w,
        @StateId("elementsBag") BagState<KV<String, String>> elementsBag) {
      elementsBag.add(element);
      // eventTimer.set(w.maxTimestamp());
      eventTimer.offset(Duration.standardSeconds(30)).setRelative();
    }

    @OnTimer("eventTimer")
    public void onTimer(
        @StateId("elementsBag") BagState<KV<String, String>> elementsBag,
        OutputReceiver<KV<String, Iterable<String>>> output) {
      String key = elementsBag.read().iterator().next().getKey().split("~")[0];
      AtomicInteger bufferSize = new AtomicInteger();
      List<String> rows = new ArrayList<>();
      elementsBag
          .read()
          .forEach(
              element -> {
                Integer elementSize = element.getValue().getBytes().length;
                boolean clearBuffer = bufferSize.intValue() + elementSize.intValue() > batchSize;
                if (clearBuffer) {
                  //numberOfRowsBagged.inc(rows.size());
                  LOG.debug("Clear Buffer {} , Key {}", bufferSize.intValue(), key);
                  output.output(KV.of(key, rows));
                  // clean up in a method
                  rows.clear();
                  bufferSize.set(0);
                  rows.add(element.getValue());
                  bufferSize.getAndAdd(Integer.valueOf(element.getValue().getBytes().length));

                } else {
                  // clean up in a method
                  rows.add(element.getValue());
                  bufferSize.getAndAdd(Integer.valueOf(element.getValue().getBytes().length));
                }
              });
      // must be  a better way
      if (!rows.isEmpty()) {
        LOG.debug("Remaining buffer {}, key{}", rows.size(), key);
        //numberOfRowsBagged.inc(rows.size());
        output.output(KV.of(key, rows));
      }
    }
  }

  public static class InspectData extends DoFn<KV<String, Iterable<String>>, Row> {
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
        ContentItem contentItem =
            ContentItem.newBuilder().setValue(emitResult(c.element().getValue())).build();
        this.requestBuilder.setItem(contentItem);
        InspectContentResponse response =
            dlpServiceClient.inspectContent(this.requestBuilder.build());
        String timeStamp = Util.getTimeStamp();
        
        boolean hasErrors = response.findInitializationErrors().stream().count() > 0;
        if (response.hasResult() && !hasErrors) {
            long bytesInspected = contentItem.getValue().getBytes().length;
        	response
              .getResult()
              .getFindingsList()
              .forEach(
                  finding -> {
                    Row row =
                        Row.withSchema(Util.bqDataSchema)
                            .addValues(
                                fileName,
                                timeStamp,
                                finding.getInfoType().getName(),
                                finding.getLikelihood().name(),
                                finding.getQuote(),
                                finding.getLocation().getCodepointRange().getStart(),
                                finding.getLocation().getCodepointRange().getEnd())
                            .build();
                    LOG.debug("Row {}", row);

                    c.output(Util.inspectData, row);
                  });
          numberOfBytesInspected.inc(bytesInspected);
          c.output(
              Util.auditData,
              Row.withSchema(Util.bqAuditSchema)
                  .addValues(fileName, timeStamp, bytesInspected, Util.INSPECTED)
                  .build());
        } else {
          response
              .findInitializationErrors()
              .forEach(
                  error -> {
                    c.output(
                        Util.errorData,
                        Row.withSchema(Util.errorSchema)
                            .addValues(fileName, timeStamp, error.toString())
                            .build());
                  });
          c.output(
              Util.auditData,
              Row.withSchema(Util.bqAuditSchema)
                  .addValues(fileName, timeStamp, 0, Util.FAILED)
                  .build());
        }
      }
    }
  }

  private static String emitResult(Iterable<String> bufferData) {
    StringBuilder builder = new StringBuilder();
    bufferData.forEach(
        e -> {
          builder.append(e);
        });
    LOG.debug("buffer data {}", builder.toString());
    return builder.toString();
  }
}
