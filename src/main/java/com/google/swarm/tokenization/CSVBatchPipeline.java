
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

package com.google.swarm.tokenization;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.FileIO.ReadableFile;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Watch;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.client.util.Charsets;
import com.google.api.services.storage.Storage;
import com.google.cloud.dlp.v2.DlpServiceClient;
import com.google.privacy.dlp.v2.ContentItem;
import com.google.privacy.dlp.v2.DeidentifyContentRequest;
import com.google.privacy.dlp.v2.DeidentifyContentResponse;
import com.google.privacy.dlp.v2.FieldId;
import com.google.privacy.dlp.v2.ProjectName;
import com.google.privacy.dlp.v2.Table;
import com.google.swarm.tokenization.common.TokenizePipelineOptions;
import com.google.swarm.tokenization.common.KMSFactory;
import com.google.swarm.tokenization.common.StorageFactory;
import com.google.swarm.tokenization.common.Util;
import com.google.swarm.tokenization.common.WriteOneFilePerWindow;

public class CSVBatchPipeline {

	public static final Logger LOG = LoggerFactory
			.getLogger(CSVBatchPipeline.class);

	@SuppressWarnings("serial")
	public static class FormatTableData extends DoFn<Table, String> {

		private boolean addHeader;
		public FormatTableData(boolean addHeader) {
			this.addHeader = addHeader;
		}

		@ProcessElement
		public void processElement(ProcessContext c) {
			Table encryptedData = c.element();
			StringBuffer bufferedWriter = new StringBuffer();
			List<FieldId> outputHeaderFields = encryptedData.getHeadersList();
			List<Table.Row> outputRows = encryptedData.getRowsList();
			List<String> outputHeaders = outputHeaderFields.stream()
					.map(FieldId::getName).collect(Collectors.toList());
			if (this.addHeader) {
				bufferedWriter.append(String.join(",", outputHeaders) + "\n");
				this.addHeader = false;
			}

			for (Table.Row outputRow : outputRows) {
				String row = outputRow.getValuesList().stream()
						.map(value -> value.getStringValue())
						.collect(Collectors.joining(","));
				bufferedWriter.append(row + "\n");
			}

			// LOG.info("Format Data: " + bufferedWriter.toString());
			c.output(bufferedWriter.toString().trim());
		}
	}

	@SuppressWarnings("serial")
	public static class DLPTableHandler extends DoFn<List<Table>, Table> {
		@ProcessElement
		public void processElement(ProcessContext c) {
			c.element().forEach(table -> {
				c.output(table);
			});
		}
	}

	@SuppressWarnings("serial")
	public static class CSVFileReader extends DoFn<ReadableFile, List<Table>> {
		private Integer batchSize;
		private String cSek;
		private String cSekhash;
		private String kmsKeyProjectName;
		private String objectName;
		private String bucketName;
		private String key;
		private boolean customerSuppliedKey;
		private String fileDecryptKey;
		private String fileDecryptKeyName;

		public CSVFileReader(ValueProvider<String> kmsKeyProjectName,
				ValueProvider<String> fileDecryptKeyRing,
				ValueProvider<String> fileDecryptKey,
				ValueProvider<Integer> batchSize, ValueProvider<String> cSek,
				ValueProvider<String> cSekhash)
				throws IOException, GeneralSecurityException {

			if (batchSize.isAccessible())
				this.batchSize = batchSize.get();

			if (kmsKeyProjectName.isAccessible())
				this.kmsKeyProjectName = kmsKeyProjectName.get();
			if (fileDecryptKey.isAccessible())
				this.fileDecryptKey = fileDecryptKey.get();

			if (fileDecryptKeyRing.isAccessible())
				this.fileDecryptKeyName = fileDecryptKeyRing.get();

			if (cSek.isAccessible())
				this.cSek = cSek.get();
			if (cSekhash.isAccessible())
				this.cSekhash = cSekhash.get();

			this.customerSuppliedKey = false;
			this.key = null;

		}

		@ProcessElement
		public void processElement(ProcessContext c)
				throws IOException, GeneralSecurityException {

			this.customerSuppliedKey = Util.findEncryptionType(
					this.fileDecryptKeyName, this.fileDecryptKey, this.cSek,
					this.cSekhash);

			if (customerSuppliedKey)
				this.key = KMSFactory.decrypt(this.kmsKeyProjectName, "global",
						this.fileDecryptKeyName, this.fileDecryptKey,
						this.cSek);

			bucketName = Util.parseBucketName(c.element().getMetadata()
					.resourceId().getCurrentDirectory().toString());

			objectName = c.element().getMetadata().resourceId().getFilename()
					.toString();

			LOG.info("Process Element:" + " Bucket Name: " + bucketName
					+ " File Name: " + objectName + " CSK"
					+ this.customerSuppliedKey + " csek: " + this.cSek
					+ " csekhash: " + this.cSekhash + " key ring name: "
					+ this.fileDecryptKeyName + " Key: " + this.fileDecryptKey);

			try {
				BufferedReader br;
				InputStream objectData = null;

				if (!this.customerSuppliedKey) {

					ReadableByteChannel channel = c.element().open();
					br = new BufferedReader(
							Channels.newReader(channel, Charsets.UTF_8.name()));

				} else {

					Storage storage = null;
					try {
						storage = StorageFactory.getService();
					} catch (GeneralSecurityException e) {
						LOG.error("Error Creating Storage API Client");
						e.printStackTrace();
					}
					try {
						objectData = StorageFactory.downloadObject(storage,
								bucketName, objectName, key, cSekhash);
					} catch (Exception e) {
						LOG.error(
								"Error Reading the Encrypted File in GCS- Customer Supplied Key");
						e.printStackTrace();
					}
					br = new BufferedReader(new InputStreamReader(objectData));

				}

				boolean endOfFile = false;
				List<FieldId> headers;
				List<Table> tables = new ArrayList<>();
				headers = Arrays.stream(br.readLine().split(",")).map(
						header -> FieldId.newBuilder().setName(header).build())
						.collect(Collectors.toList());

				while (!endOfFile) {
					List<String> lines = Util.readBatch(br, this.batchSize);
					Table batchData = Util.createDLPTable(headers, lines);
					tables.add(batchData);
					if (lines.size() < batchSize) {
						endOfFile = true;
					}
				}
				br.close();
				if (objectData != null)
					objectData.close();
				c.output(tables);

			} catch (IOException e) {
				LOG.error("Error Reading the File " + e.getMessage());
				e.printStackTrace();
				System.exit(1);

			}
		}
	}

	@SuppressWarnings("serial")
	public static class TokenizeData extends DoFn<Table, Table> {

		private String projectId;
		private String deIdentifyTemplateName;
		private String inspectTemplateName;
		private boolean inspectTemplateExist;

		public TokenizeData(ValueProvider<String> projectId,
				ValueProvider<String> deIdentifyTemplateName,
				ValueProvider<String> inspectTemplateName) {
			if (projectId.isAccessible())
				this.projectId = projectId.get();
			if (deIdentifyTemplateName.isAccessible())
				this.deIdentifyTemplateName = deIdentifyTemplateName.get();
			if (inspectTemplateName.isAccessible()) {
				this.inspectTemplateName = inspectTemplateName.get();
				this.inspectTemplateExist = true;
			} else {
				this.inspectTemplateExist = false;
			}
		}

		@ProcessElement
		public void processElement(ProcessContext c) {
			Table nonEncryptedData = c.element();
			Table encryptedData;
			try (DlpServiceClient dlpServiceClient = DlpServiceClient
					.create()) {

				ContentItem tableItem = ContentItem.newBuilder()
						.setTable(nonEncryptedData).build();
				DeidentifyContentRequest request;
				DeidentifyContentResponse response;

				if (this.inspectTemplateExist) {
					request = DeidentifyContentRequest.newBuilder()
							.setParent(
									ProjectName.of(this.projectId).toString())
							.setDeidentifyTemplateName(
									this.deIdentifyTemplateName)
							.setInspectTemplateName(this.inspectTemplateName)
							.setItem(tableItem).build();
				} else {
					request = DeidentifyContentRequest.newBuilder()
							.setParent(
									ProjectName.of(this.projectId).toString())
							.setDeidentifyTemplateName(
									this.deIdentifyTemplateName)
							.setItem(tableItem).build();

				}
				response = dlpServiceClient.deidentifyContent(request);
				encryptedData = response.getItem().getTable();
				LOG.info("Request Size Successfully Tokenized: "
						+ request.toByteString().size() + " bytes");
				c.output(encryptedData);

			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	public static void main(String[] args)
			throws IOException, GeneralSecurityException {

		TokenizePipelineOptions options = PipelineOptionsFactory.fromArgs(args)
				.withValidation().as(TokenizePipelineOptions.class);

		Pipeline p = Pipeline.create(options);
		p.apply(FileIO.match().filepattern(options.getInputFile())
				// 10 seconds polling
				.continuously(Duration.standardSeconds(10),
						Watch.Growth.never()))
				.apply(FileIO.readMatches()
						.withCompression(Compression.UNCOMPRESSED))
				.apply("CSV File Reader",
						ParDo.of(new CSVFileReader(options.getDlpProject(),
								options.getFileDecryptKeyName(),
								options.getFileDecryptKey(),
								options.getBatchSize(), options.getCsek(),
								options.getCsekhash())))
				.apply("DLP Table Handler", ParDo.of(new DLPTableHandler()))
				.apply("Tokenize Data",
						ParDo.of(new TokenizeData(options.getDlpProject(),
								options.getDeidentifyTemplateName(),
								options.getInspectTemplateName())))
				.apply("Format Table Data", ParDo.of(new FormatTableData(true)))
				// 1 minute window
				.apply(Window.<String>into(
						FixedWindows.of(Duration.standardMinutes(1))))
				.apply(new WriteOneFilePerWindow(options.getOutputFile(), 1));

		p.run();
	}

}
