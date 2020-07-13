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
import com.google.privacy.dlp.v2.DeidentifyContentRequest;
import com.google.privacy.dlp.v2.DeidentifyContentResponse;
import com.google.privacy.dlp.v2.FieldId;
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
public class DeidentifyData
    extends DoFn<KV<String, Iterable<Table.Row>>, KV<String, DeidentifyContentResponse>> {
  private final String projectId;
  private final String inspectTemplateName;
  private final String deidentifyTemplateName;
  private final PCollectionView<List<String>> headerColumns;
  private transient DeidentifyContentRequest.Builder requestBuilder;
  private transient DlpServiceClient dlpServiceClient;

  @Setup
  public void setup() throws IOException {
    requestBuilder =
        DeidentifyContentRequest.newBuilder().setParent(ProjectName.of(projectId).toString());
    if (inspectTemplateName != null) {
      requestBuilder.setInspectTemplateName(inspectTemplateName);
    }

    if (deidentifyTemplateName != null) {
      requestBuilder.setDeidentifyTemplateName(deidentifyTemplateName);
    }
    dlpServiceClient = DlpServiceClient.create();
  }

  @Teardown
  public void teardown() {
    dlpServiceClient.close();
  }

  public DeidentifyData(
      String projectId,
      String inspectTemplateName,
      String deidentifyTemplateName,
      PCollectionView<List<String>> headerColumns) {
    this.projectId = projectId;
    this.inspectTemplateName = inspectTemplateName;
    this.deidentifyTemplateName = deidentifyTemplateName;
    this.headerColumns = headerColumns;
  }

  @ProcessElement
  public void processElement(ProcessContext c) throws IOException {
    String fileName = c.element().getKey();
    List<FieldId> dlpTableHeaders;
    if (headerColumns != null) {
      dlpTableHeaders =
          c.sideInput(headerColumns).stream()
              .map(header -> FieldId.newBuilder().setName(header).build())
              .collect(Collectors.toList());
    } else {
      // handle unstructured input
      dlpTableHeaders = new ArrayList<>();
      dlpTableHeaders.add(FieldId.newBuilder().setName("value").build());
    }
    Table table =
        Table.newBuilder()
            .addAllHeaders(dlpTableHeaders)
            .addAllRows(c.element().getValue())
            .build();
    ContentItem contentItem = ContentItem.newBuilder().setTable(table).build();
    this.requestBuilder.setItem(contentItem);
    DeidentifyContentResponse response =
        dlpServiceClient.deidentifyContent(this.requestBuilder.build());
    c.output(KV.of(fileName, response));
  }
}
