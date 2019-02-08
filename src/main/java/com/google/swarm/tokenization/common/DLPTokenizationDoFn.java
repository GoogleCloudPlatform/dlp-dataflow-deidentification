/*
 * Copyright 2018 Google LLC
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
import java.sql.SQLException;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.cloud.dlp.v2.DlpServiceClient;
import com.google.privacy.dlp.v2.ContentItem;
import com.google.privacy.dlp.v2.DeidentifyContentRequest;
import com.google.privacy.dlp.v2.DeidentifyContentResponse;
import com.google.privacy.dlp.v2.FieldId;
import com.google.privacy.dlp.v2.ProjectName;
import com.google.privacy.dlp.v2.Table;

public class DLPTokenizationDoFn extends DoFn<KV<String, Table>, Row> {

	/**
	 * 
	 */
	private static final long serialVersionUID = -8845993157503721016L;
	public static final Logger LOG = LoggerFactory.getLogger(DLPTokenizationDoFn.class);
	private String projectId;
	private DlpServiceClient dlpServiceClient;
	private ValueProvider<String> deIdentifyTemplateName;
	private ValueProvider<String> inspectTemplateName;
	private boolean inspectTemplateExist;

	public DLPTokenizationDoFn(String projectId, ValueProvider<String> deIdentifyTemplateName,
			ValueProvider<String> inspectTemplateName) {
		this.projectId = projectId;
		dlpServiceClient = null;
		this.deIdentifyTemplateName = deIdentifyTemplateName;
		this.inspectTemplateName = inspectTemplateName;
		inspectTemplateExist = false;

	}

	@StartBundle
	public void startBundle() throws SQLException {

		try {
			this.dlpServiceClient = DlpServiceClient.create();

		} catch (IOException e) {

			e.printStackTrace();
		}

	}

	@FinishBundle
	public void finishBundle() throws Exception {
		if (this.dlpServiceClient != null) {
			this.dlpServiceClient.close();
		}
	}

	@ProcessElement
	public void processElement(ProcessContext c) {

		String key = c.element().getKey();
		Table nonEncryptedData = c.element().getValue();
		if (this.inspectTemplateName.isAccessible()) {
			if (this.inspectTemplateName.get() != null)
				this.inspectTemplateExist = true;
		}
		ContentItem tableItem = ContentItem.newBuilder().setTable(nonEncryptedData).build();
		DeidentifyContentRequest request;
		DeidentifyContentResponse response;
		if (this.inspectTemplateExist) {
			request = DeidentifyContentRequest.newBuilder().setParent(ProjectName.of(this.projectId).toString())
					.setDeidentifyTemplateName(this.deIdentifyTemplateName.get())
					.setInspectTemplateName(this.inspectTemplateName.get()).setItem(tableItem).build();
		} else {
			request = DeidentifyContentRequest.newBuilder().setParent(ProjectName.of(this.projectId).toString())
					.setDeidentifyTemplateName(this.deIdentifyTemplateName.get()).setItem(tableItem).build();

		}

		response = dlpServiceClient.deidentifyContent(request);
		Table encryptedData = response.getItem().getTable();
		LOG.info("Request Size Successfully Tokenized:{} rows {} bytes ", encryptedData.getRowsList().size(),
				request.toByteString().size());

		List<String> outputHeaders = encryptedData.getHeadersList().stream().map(FieldId::getName)
				.collect(Collectors.toList());
		String[] header = new String[outputHeaders.size()];

		for (int i = 0; i < header.length; i++) {
			header[i] = Util.checkHeaderName(outputHeaders.get(i));

		}
		List<Table.Row> outputRows = encryptedData.getRowsList();

		for (Table.Row outputRow : outputRows) {

			String dlpRow = outputRow.getValuesList().stream().map(value -> value.getStringValue())
					.collect(Collectors.joining(","));
			String[] values = dlpRow.split(",");
			Row row = new Row(key, header, values);
			c.output(row);
		}
	}
}
