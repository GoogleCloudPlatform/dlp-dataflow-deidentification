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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

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
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.Row;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.auto.value.AutoValue;
import com.google.cloud.dlp.v2.DlpServiceClient;
import com.google.privacy.dlp.v2.ContentItem;
import com.google.privacy.dlp.v2.FieldId;
import com.google.privacy.dlp.v2.InspectContentRequest;
import com.google.privacy.dlp.v2.InspectContentResponse;
import com.google.privacy.dlp.v2.ProjectName;
import com.google.privacy.dlp.v2.Table;
import com.google.privacy.dlp.v2.Value;

@AutoValue
public abstract class DLPTransform
    extends PTransform<PCollection<KV<String, String>>, PCollection<Row>> {
  public static final Logger LOG = LoggerFactory.getLogger(DLPTransform.class);

  public abstract String inspectTemplateName();

  public abstract Integer batchSize();

  public abstract String projectId();

  public abstract PCollectionView<List<String>> csvHeader();

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder setInspectTemplateName(String inspectTemplateName);

    public abstract Builder setBatchSize(Integer batchSize);

    public abstract Builder setProjectId(String projectId);

    public abstract Builder setCsvHeader(PCollectionView<List<String>> csvHeader);

    public abstract DLPTransform build();
  }

  public static Builder newBuilder() {
    return new AutoValue_DLPTransform.Builder();
  }

  @Override
  public PCollection<Row> expand(PCollection<KV<String, String>> input) {
    return input
        .apply("ConvertToDLPRow", ParDo.of(new ConvertToDLPRow()))
        .apply("Batch Contents", ParDo.of(new BatchTableRequest(batchSize())))
        .apply(
            "ConvertToDLPTable",
            ParDo.of(new ConvertToDLPTable(csvHeader())).withSideInputs(csvHeader()))
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

  public static class InspectData extends DoFn<KV<String, Table>, Row> {
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
        Table table = c.element().getValue();
        LOG.info("Row Count {}", table.getRowsCount());
        ContentItem contentItem = ContentItem.newBuilder().setTable(table).build();
        this.requestBuilder.setItem(contentItem);
        InspectContentResponse response =
            dlpServiceClient.inspectContent(this.requestBuilder.build());
        response
            .getResult()
            .getFindingsList()
            .forEach(
                finding -> {
                  LOG.info("Finding {}", finding.toString());

                  Row row =
                      Row.withSchema(Util.dlpInspectionSchema)
                          .addValues(
                              fileName,
                              Util.getTimeStamp(),
                              finding.getInfoType().getName(),
                              finding.getLikelihood().name(),
                              finding
                                  .getLocation()
                                  .getContentLocationsList()
                                  .get(0)
                                  .getRecordLocation()
                                  .getFieldId()
                                  .getName(),
                              finding.getQuote(),
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

  public static class BatchTableRequest
      extends DoFn<KV<String, Table.Row>, Iterable<KV<String, Table.Row>>> {

    private static final long serialVersionUID = 1L;
    private final Counter numberOfRowsBagged =
        Metrics.counter(BatchTableRequest.class, "numberOfRowsBagged");
    private Integer batchSize;

    public BatchTableRequest(Integer batchSize) {
      this.batchSize = batchSize;
    }

    @StateId("elementsBag")
    private final StateSpec<BagState<KV<String, Table.Row>>> elementsBag = StateSpecs.bag();

    @StateId("elementsSize")
    private final StateSpec<ValueState<Integer>> elementsSize = StateSpecs.value();

    @TimerId("eventTimer")
    private final TimerSpec eventTimer = TimerSpecs.timer(TimeDomain.EVENT_TIME);

    @ProcessElement
    public void process(
        @Element KV<String, Table.Row> element,
        @StateId("elementsBag") BagState<KV<String, Table.Row>> elementsBag,
        @StateId("elementsSize") ValueState<Integer> elementsSize,
        @Timestamp Instant elementTs,
        @TimerId("eventTimer") Timer eventTimer,
        OutputReceiver<Iterable<KV<String, Table.Row>>> output) {
      eventTimer.set(elementTs);
      Integer currentElementSize =
          (element.getValue() == null) ? 0 : element.getValue().getSerializedSize();
      Integer currentBufferSize = (elementsSize.read() == null) ? 0 : elementsSize.read();
      boolean clearBuffer = (currentElementSize + currentBufferSize) > batchSize;
      LOG.debug(
          "Clear Buffer {}, Curret Elements Size {}, currentBufferSize {}",
          clearBuffer,
          currentElementSize,
          currentBufferSize);
      if (clearBuffer) {
        output.output(elementsBag.read());
        LOG.debug("****CLEAR BUFFER **** Current Buffer Size {}", elementsSize.read());
        clearState(elementsBag, elementsSize);
        clearBuffer = false;
        currentBufferSize = 0;
        addState(elementsBag, elementsSize, element, currentElementSize + currentBufferSize);
        numberOfRowsBagged.inc();

      } else {
        addState(elementsBag, elementsSize, element, currentElementSize + currentBufferSize);
        numberOfRowsBagged.inc();
      }
    }

    @OnTimer("eventTimer")
    public void onTimer(
        @StateId("elementsBag") BagState<KV<String, Table.Row>> elementsBag,
        @StateId("elementsSize") ValueState<Integer> elementsSize,
        OutputReceiver<Iterable<KV<String, Table.Row>>> output) {
      if (elementsSize.read() < batchSize) output.output(elementsBag.read());
      else {
        LOG.error("Element Size {} is Larger than batch size {}", elementsSize.read(), batchSize);
      }
      LOG.debug("****Timer Triggered **** Current Buffer Size {}", elementsSize.read(), batchSize);
    }

    private static void clearState(
        BagState<KV<String, Table.Row>> elementsBag, ValueState<Integer> elementsSize) {
      elementsBag.clear();
      elementsSize.clear();
    }

    private static void addState(
        BagState<KV<String, Table.Row>> elementsBag,
        ValueState<Integer> elementsSize,
        KV<String, Table.Row> element,
        Integer size) {
      elementsBag.add(element);
      elementsSize.write(size);
    }
  }

  public static class ConvertToDLPRow extends DoFn<KV<String, String>, KV<String, Table.Row>> {

    @ProcessElement
    public void processElement(ProcessContext c) {

      String row = c.element().getValue();
      String key = c.element().getKey();
      Table.Row.Builder tableRowBuilder = Table.Row.newBuilder();

      tableRowBuilder.addValues(Value.newBuilder().setStringValue(row));

      Table.Row dlpRow = tableRowBuilder.build();
      LOG.debug("Key {}, DLPRow {}", key, dlpRow);
      c.output(KV.of(key, dlpRow));
    }
  }

  private static KV<String, Table> emitTableResult(
      Iterable<KV<String, Table.Row>> bufferData, List<String> csvHeaders) {

    List<Table.Row> rows = new ArrayList<>();
    Table dlpTable = null;

    String fileName =
        (bufferData.iterator().hasNext()) ? bufferData.iterator().next().getKey() : "UNKNOWN_FILE";

    // Building dlp Table Headers
    List<FieldId> dlpTableHeaders =
        csvHeaders.stream()
            .map(header -> FieldId.newBuilder().setName(header).build())
            .collect(Collectors.toList());

    dlpTable = Table.newBuilder().addAllHeaders(dlpTableHeaders).addAllRows(rows).build();
    LOG.info("rows {}, header {}", rows.size(), dlpTableHeaders.size());
    return KV.of(fileName, dlpTable);
  }

  public static class ConvertToDLPTable
      extends DoFn<Iterable<KV<String, Table.Row>>, KV<String, Table>> {
    private PCollectionView<List<String>> csvHeader;

    public ConvertToDLPTable(PCollectionView<List<String>> csvHeader) {
      this.csvHeader = csvHeader;
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
      c.output(emitTableResult(c.element(), c.sideInput(csvHeader)));
    }
  }
}
