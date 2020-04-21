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
import com.google.privacy.dlp.v2.FieldId;
import com.google.privacy.dlp.v2.InspectContentRequest;
import com.google.privacy.dlp.v2.InspectContentResponse;
import com.google.privacy.dlp.v2.ProjectName;
import com.google.privacy.dlp.v2.Table;
import com.google.privacy.dlp.v2.Value;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
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
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@AutoValue
public abstract class DLPTransform
    extends PTransform<PCollection<KV<String, String>>, PCollection<Row>> {
  public static final Logger LOG = LoggerFactory.getLogger(DLPTransform.class);

  public abstract String inspectTemplateName();

  public abstract Integer batchSize();

  public abstract String projectId();

  public abstract String columnDelimeter();

  public abstract PCollectionView<List<String>> csvHeader();

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder setInspectTemplateName(String inspectTemplateName);

    public abstract Builder setBatchSize(Integer batchSize);

    public abstract Builder setProjectId(String projectId);

    public abstract Builder setCsvHeader(PCollectionView<List<String>> csvHeader);

    public abstract Builder setColumnDelimeter(String columnDelimeter);

    public abstract DLPTransform build();
  }

  public static Builder newBuilder() {
    return new AutoValue_DLPTransform.Builder();
  }

  @Override
  public PCollection<Row> expand(PCollection<KV<String, String>> input) {
    return input
        .apply("ConvertToDLPRow", ParDo.of(new ConvertToDLPRow(columnDelimeter())))
        .apply("Batch Contents", ParDo.of(new BatchTableRequest(batchSize())))
        .apply(
            "DLPInspect",
            ParDo.of(new InspectData(projectId(), inspectTemplateName(), csvHeader()))
                .withSideInputs(csvHeader()));
  }

  public static class InspectData extends DoFn<KV<String, Iterable<Table.Row>>, Row> {
    private String projectId;
    private String inspectTemplateName;
    PCollectionView<List<String>> csvHeader;
    List<FieldId> dlpTableHeaders = null;
    private InspectContentRequest.Builder requestBuilder;
    private final Counter numberOfBytesInspected =
        Metrics.counter(InspectData.class, "NumberOfBytesInspected");
    private final Counter numberOfRowsInspected =
        Metrics.counter(InspectData.class, "NumberOfRowsInspected");

    public InspectData(
        String projectId, String inspectTemplateName, PCollectionView<List<String>> csvHeader) {
      this.projectId = projectId;
      this.inspectTemplateName = inspectTemplateName;
      this.csvHeader = csvHeader;
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
        dlpTableHeaders =
            c.sideInput(csvHeader).stream()
                .map(header -> FieldId.newBuilder().setName(header).build())
                .collect(Collectors.toList());
        Table table =
            Table.newBuilder()
                .addAllHeaders(dlpTableHeaders)
                .addAllRows(c.element().getValue())
                .build();
        ContentItem contentItem = ContentItem.newBuilder().setTable(table).build();
        this.requestBuilder.setItem(contentItem);
        InspectContentResponse response =
            dlpServiceClient.inspectContent(this.requestBuilder.build());
        String timeStamp = Util.getTimeStamp();
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
                                timeStamp,
                                finding.getInfoType().getName(),
                                finding.getLikelihood().name(),
                                finding.getQuote(),
                                finding.getLocation().getCodepointRange().getStart(),
                                finding.getLocation().getCodepointRange().getEnd())
                            .build();
                    LOG.info("Row{}",row);
                    c.output(row);
                  });
        } else {
        	LOG.info("response has no result");
        }
        numberOfRowsInspected.inc(contentItem.getTable().getRowsCount());
        numberOfBytesInspected.inc(contentItem.getSerializedSize());
      }
    }
  }

  public static class BatchTableRequest
      extends DoFn<KV<String, Table.Row>, KV<String, Iterable<Table.Row>>> {

    private static final long serialVersionUID = 1L;
    private final Counter numberOfRowsBagged =
        Metrics.counter(BatchTableRequest.class, "numberOfRowsBagged");
    private Integer batchSize;

    public BatchTableRequest(Integer batchSize) {
      this.batchSize = batchSize;
    }

    @StateId("elementsBag")
    private final StateSpec<BagState<KV<String, Table.Row>>> elementsBag = StateSpecs.bag();

    @TimerId("eventTimer")
    private final TimerSpec eventTimer = TimerSpecs.timer(TimeDomain.EVENT_TIME);

    @ProcessElement
    public void process(
        @Element KV<String, Table.Row> element,
        @StateId("elementsBag") BagState<KV<String, Table.Row>> elementsBag,
        @TimerId("eventTimer") Timer eventTimer,
        BoundedWindow w,
        ProcessContext c) {
      elementsBag.add(element);
      eventTimer.set(w.maxTimestamp());
    }

    @OnTimer("eventTimer")
    public void onTimer(
        @StateId("elementsBag") BagState<KV<String, Table.Row>> elementsBag,
        OutputReceiver<KV<String, Iterable<Table.Row>>> output) {
      String key = elementsBag.read().iterator().next().getKey();
      AtomicInteger bufferSize = new AtomicInteger();
	  List<Table.Row> rows = new ArrayList<>();
      elementsBag
          .read()
          .forEach(
              element -> {
                Integer elementSize = element.getValue().getSerializedSize();
                boolean clearBuffer = bufferSize.intValue() + elementSize.intValue() > batchSize;
                if (clearBuffer) {
                  numberOfRowsBagged.inc();
                  LOG.info("Clear Buffer {} , Key {}",bufferSize.intValue(),element.getKey());
                  output.output(KV.of(element.getKey(), rows));
                  // clean up in a method
                  rows.clear();
                  bufferSize.set(0);
                  rows.add(element.getValue());
                  bufferSize.getAndAdd(Integer.valueOf(element.getValue().getSerializedSize()));

                } else {
                  // clean up in a method
                  rows.add(element.getValue());
                  bufferSize.getAndAdd(Integer.valueOf(element.getValue().getSerializedSize()));
                }
              });
      // must be  a better way
      if (!rows.isEmpty()) {
    	  LOG.info("Remaining rows {}",rows.size());	
          numberOfRowsBagged.inc();
    	  output.output(KV.of(key, rows));
      }
    
    }
  }

  public static class ConvertToDLPRow extends DoFn<KV<String, String>, KV<String, Table.Row>> {

    private String columnDelimeter;

    public ConvertToDLPRow(String columnDelimeter) {
      this.columnDelimeter = columnDelimeter;
    }

    @ProcessElement
    public void processElement(ProcessContext c) {

      String row = c.element().getValue();
      String key = c.element().getKey();
      List<String> rows = Arrays.asList(row.split(columnDelimeter));
      Table.Row.Builder tableRowBuilder = Table.Row.newBuilder();
      rows.forEach(
          r -> {
            tableRowBuilder.addValues(Value.newBuilder().setStringValue(r));
          });

      Table.Row dlpRow = tableRowBuilder.build();
      c.output(KV.of(key, dlpRow));
    }
  }
}
