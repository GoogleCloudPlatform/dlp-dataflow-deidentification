
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

import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.http.HttpHeaders;
import com.google.api.client.util.Charsets;
import com.google.api.services.cloudkms.v1.CloudKMS;
import com.google.api.services.cloudkms.v1.model.DecryptRequest;
import com.google.api.services.cloudkms.v1.model.DecryptResponse;
import com.google.api.services.storage.Storage;
import com.google.cloud.dlp.v2.DlpServiceClient;
import com.google.privacy.dlp.v2.ContentItem;
import com.google.privacy.dlp.v2.DeidentifyContentRequest;
import com.google.privacy.dlp.v2.DeidentifyContentResponse;
import com.google.privacy.dlp.v2.FieldId;
import com.google.privacy.dlp.v2.ProjectName;
import com.google.privacy.dlp.v2.Table;
import com.google.privacy.dlp.v2.Value;
import com.google.swarm.tokenization.common.CSVPipelineOptions;
import com.google.swarm.tokenization.common.KMSFactory;
import com.google.swarm.tokenization.common.StorageFactory;
import com.google.swarm.tokenization.common.WriteOneFilePerWindow;

public class CSVBatchPipeline {

	public static final Logger LOG = LoggerFactory
			.getLogger(CSVBatchPipeline.class);

	// convert CSV row into Table.Row
	private static Table.Row convertCsvRowToTableRow(String row) {
		String[] values = row.split(",");
		Table.Row.Builder tableRowBuilder = Table.Row.newBuilder();
		for (String value : values) {
			tableRowBuilder.addValues(
					Value.newBuilder().setStringValue(value).build());
		}

		return tableRowBuilder.build();
	}

	@SuppressWarnings("serial")
	public static class FormatTableData extends DoFn<Table, String> {

		@ProcessElement
		public void processElement(ProcessContext c) {
			Table encryptedData = c.element();
			StringBuffer bufferedWriter = new StringBuffer();
			List<FieldId> outputHeaderFields = encryptedData.getHeadersList();
			List<Table.Row> outputRows = encryptedData.getRowsList();
			List<String> outputHeaders = outputHeaderFields.stream()
					.map(FieldId::getName).collect(Collectors.toList());
			bufferedWriter.append(String.join(",", outputHeaders) + "\n");
			for (Table.Row outputRow : outputRows) {
				String row = outputRow.getValuesList().stream()
						.map(value -> value.getStringValue())
						.collect(Collectors.joining(","));
				bufferedWriter.append(row + "\n");
			}

			// LOG.info("Format Data: " + bufferedWriter.toString());
			c.output(bufferedWriter.toString());
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

	public static InputStream downloadObject(Storage storage, String bucketName,
			String objectName, String base64CseKey, String base64CseKeyHash)
			throws Exception {

		// Set the CSEK headers
		final HttpHeaders httpHeaders = new HttpHeaders();
		httpHeaders.set("x-goog-encryption-algorithm", "AES256");
		httpHeaders.set("x-goog-encryption-key", base64CseKey);
		httpHeaders.set("x-goog-encryption-key-sha256", base64CseKeyHash);
		Storage.Objects.Get getObject = storage.objects().get(bucketName,
				objectName);
		getObject.setRequestHeaders(httpHeaders);

		try {

			return getObject.executeMediaAsInputStream();
		} catch (GoogleJsonResponseException e) {
			LOG.info("Error downloading: " + e.getContent());
			System.exit(1);
			return null;
		}
	}

	private static String parseBucketName(String value) {
		// gs://name/ -> name
		return value.substring(5, value.length() - 1);
	}

	public static String decrypt(String projectId, String locationId,
			String keyRingId, String cryptoKeyId, String ciphertext)
			throws IOException, GeneralSecurityException {
		// Create the Cloud KMS client.
		CloudKMS kms = KMSFactory.getService();
		// The resource name of the cryptoKey
		String cryptoKeyName = String.format(
				"projects/%s/locations/%s/keyRings/%s/cryptoKeys/%s", projectId,
				locationId, keyRingId, cryptoKeyId);

		DecryptRequest request = new DecryptRequest().setCiphertext(ciphertext);
		DecryptResponse response = kms.projects().locations().keyRings()
				.cryptoKeys().decrypt(cryptoKeyName, request).execute();

		return response.getPlaintext().toString();
	}

	@SuppressWarnings("serial")
	public static class CSVFileReader extends DoFn<ReadableFile, List<Table>> {
		private ValueProvider<Integer> batchSize;
		private ValueProvider<String> cSek;
		private ValueProvider<String> cSekhash;
		private String objectName;
		private String bucketName;
		private String key;

		public CSVFileReader(ValueProvider<String> kmsKeyProjectName,
				ValueProvider<String> fileDecryptKeyRing,
				ValueProvider<String> fileDecryptKey,
				ValueProvider<Integer> batchSize, ValueProvider<String> cSek,
				ValueProvider<String> cSekhash)
				throws IOException, GeneralSecurityException {
			this.batchSize = batchSize;
			this.key = decrypt(kmsKeyProjectName.get(), "global",
					fileDecryptKeyRing.get(), fileDecryptKey.get(),
					cSek.get().toString());
			this.cSek = cSek;
			this.cSekhash = cSekhash;

		}

		@ProcessElement
		public void processElement(ProcessContext c) {

			objectName = c.element().getMetadata().resourceId().getFilename()
					.toString();
			bucketName = parseBucketName(c.element().getMetadata().resourceId()
					.getCurrentDirectory().toString());
			LOG.info(
					"Bucket Name: " + bucketName + " File Name: " + objectName);

			try {
				BufferedReader br;
				InputStream objectData = null;

				if (cSek.get().toString().equals("null")
						|| cSekhash.get().toString().equals("null")) {

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
						objectData = downloadObject(storage, bucketName,
								objectName, key, cSekhash.get().toString());
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
					List<String> lines = readBatch(br, this.batchSize);
					Table batchData = createDLPTable(headers, lines);
					tables.add(batchData);
					if (lines.size() < batchSize.get().intValue()) {
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

		final ValueProvider<String> projectId;
		final ValueProvider<String> deIdentifyTemplateName;
		final ValueProvider<String> inspectTemplateName;
		boolean inspectTemplateExist;

		public TokenizeData(ValueProvider<String> projectId,
				ValueProvider<String> deIdentifyTemplateName,
				ValueProvider<String> inspectTemplateName) {
			this.projectId = projectId;
			this.deIdentifyTemplateName = deIdentifyTemplateName;
			this.inspectTemplateName = inspectTemplateName;
			// result of a bug in DLP template.
			if (inspectTemplateName.get().toString()
					.equals(new String("null"))) {
				this.inspectTemplateExist = false;
			} else {
				this.inspectTemplateExist = true;
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
							.setParent(ProjectName
									.of(this.projectId.get().toString())
									.toString())
							.setDeidentifyTemplateName(
									this.deIdentifyTemplateName.get()
											.toString())
							.setInspectTemplateName(
									this.inspectTemplateName.get().toString())
							.setItem(tableItem).build();
				} else {
					request = DeidentifyContentRequest.newBuilder()
							.setParent(ProjectName
									.of(this.projectId.get().toString())
									.toString())
							.setDeidentifyTemplateName(
									this.deIdentifyTemplateName.get()
											.toString())
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

	private static List<String> readBatch(BufferedReader reader,
			ValueProvider<Integer> batchSize) throws IOException {
		List<String> result = new ArrayList<>();

		for (int i = 0; i < batchSize.get().intValue(); i++) {
			String line = reader.readLine();
			if (line != null) {
				result.add(line);
			} else {
				return result;
			}
		}
		return result;
	}

	private static Table createDLPTable(List<FieldId> headers,
			List<String> lines) {

		List<Table.Row> rows = new ArrayList<>();
		lines.forEach(line -> {
			rows.add(convertCsvRowToTableRow(line));
		});
		Table table = Table.newBuilder().addAllHeaders(headers).addAllRows(rows)
				.build();

		return table;

	}

	public static void main(String[] args)
			throws IOException, GeneralSecurityException {

		CSVPipelineOptions options = PipelineOptionsFactory.fromArgs(args)
				.withValidation().as(CSVPipelineOptions.class);

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
				.apply("Format Table Data", ParDo.of(new FormatTableData()))
				// 1 minute window
				.apply(Window.<String>into(
						FixedWindows.of(Duration.standardMinutes(1))))
				.apply(new WriteOneFilePerWindow(options.getOutputFile().get(),
						1));

		p.run().waitUntilFinish();
	}

}
