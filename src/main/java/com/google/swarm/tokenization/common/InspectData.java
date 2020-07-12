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

import com.google.cloud.dlp.v2.DlpServiceClient;
import com.google.privacy.dlp.v2.ContentItem;
import com.google.privacy.dlp.v2.FieldId;
import com.google.privacy.dlp.v2.InspectContentRequest;
import com.google.privacy.dlp.v2.InspectContentResponse;
import com.google.privacy.dlp.v2.ProjectName;
import com.google.privacy.dlp.v2.Table;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;

@SuppressWarnings("serial")
public class InspectData
    extends DoFn<KV<String, Iterable<Table.Row>>, KV<String, InspectContentResponse>> {
  private final String projectId;
  private final String inspectTemplateName;
  private final PCollectionView<List<String>> headerColumns;
  private transient DlpServiceClient dlpServiceClient;
  private transient InspectContentRequest.Builder requestBuilder;

  public InspectData(
      String projectId, String inspectTemplateName, PCollectionView<List<String>> headerColumns) {
    this.projectId = projectId;
    this.inspectTemplateName = inspectTemplateName;
    this.headerColumns = headerColumns;
  }

  @Setup
  public void setup() throws IOException {
    this.requestBuilder =
        InspectContentRequest.newBuilder().setParent(ProjectName.of(this.projectId).toString());
    if (inspectTemplateName != null) {
      requestBuilder.setInspectTemplateName(this.inspectTemplateName);
    }

    dlpServiceClient = DlpServiceClient.create();
  }

  @Teardown
  public void teardown() {
    dlpServiceClient.close();
  }

  @ProcessElement
  public void processElement(ProcessContext c) throws IOException {
    List<FieldId> tableHeaders;
    if (headerColumns != null) {
      tableHeaders =
          c.sideInput(headerColumns).stream()
              .map(header -> FieldId.newBuilder().setName(header).build())
              .collect(Collectors.toList());
    } else {
      tableHeaders = new ArrayList<>();
      tableHeaders.add(FieldId.newBuilder().setName("value").build());
    }
    Table table =
        Table.newBuilder().addAllHeaders(tableHeaders).addAllRows(c.element().getValue()).build();
    ContentItem contentItem = ContentItem.newBuilder().setTable(table).build();
    this.requestBuilder.setItem(contentItem);
    InspectContentResponse response = dlpServiceClient.inspectContent(this.requestBuilder.build());
    c.output(KV.of(c.element().getKey(), response));
  }
}
