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
import com.google.privacy.dlp.v2.ProjectName;
import com.google.privacy.dlp.v2.ReidentifyContentRequest;
import com.google.privacy.dlp.v2.ReidentifyContentResponse;
import com.google.privacy.dlp.v2.Table;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.ProcessContext;
import org.apache.beam.sdk.transforms.DoFn.ProcessElement;
import org.apache.beam.sdk.transforms.DoFn.Setup;
import org.apache.beam.sdk.transforms.DoFn.Teardown;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("serial")
public class ReidentifyData
    extends DoFn<KV<String, Iterable<Table.Row>>, KV<String, ReidentifyContentResponse>> {
  public static final Logger LOG = LoggerFactory.getLogger(ReidentifyData.class);

  private final String projectId;
  private final String inspectTemplateName;
  private final String reidentifyTemplateName;
  private final PCollectionView<List<String>> headerColumns;
  private transient ReidentifyContentRequest.Builder requestBuilder;
  private transient DlpServiceClient dlpServiceClient;

  @Setup
  public void setup() throws IOException {
    requestBuilder =
        ReidentifyContentRequest.newBuilder().setParent(ProjectName.of(projectId).toString());
    if (inspectTemplateName != null) {
      requestBuilder.setInspectTemplateName(inspectTemplateName);
    }

    if (reidentifyTemplateName != null) {
      requestBuilder.setReidentifyTemplateName(reidentifyTemplateName);
    }
    dlpServiceClient = DlpServiceClient.create();
  }

  @Teardown
  public void teardown() {
    dlpServiceClient.close();
  }

  public ReidentifyData(
      String projectId,
      String inspectTemplateName,
      String reidentifyTemplateName,
      PCollectionView<List<String>> headerColumns) {
    this.projectId = projectId;
    this.inspectTemplateName = inspectTemplateName;
    this.reidentifyTemplateName = reidentifyTemplateName;
    this.headerColumns = headerColumns;
  }

  @ProcessElement
  public void processElement(ProcessContext context) throws IOException {
    List<FieldId> tableHeaders = new ArrayList<>();
    if (headerColumns != null) {
      tableHeaders =
          context.sideInput(headerColumns).stream()
              .map(header -> FieldId.newBuilder().setName(header).build())
              .collect(Collectors.toList());
    }
    Table table =
        Table.newBuilder()
            .addAllHeaders(tableHeaders)
            .addAllRows(context.element().getValue())
            .build();

    ContentItem contentItem = ContentItem.newBuilder().setTable(table).build();
    this.requestBuilder.setItem(contentItem);
    ReidentifyContentResponse response = dlpServiceClient.reidentifyContent(requestBuilder.build());
    context.output(KV.of(context.element().getKey(), response));
  }
}
