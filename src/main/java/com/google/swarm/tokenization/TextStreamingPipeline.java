
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

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.FileIO.ReadableFile;
import org.apache.beam.sdk.io.TextIO;
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
import com.google.api.services.cloudkms.v1.CloudKMS;
import com.google.api.services.cloudkms.v1.model.DecryptRequest;
import com.google.api.services.cloudkms.v1.model.DecryptResponse;
import com.google.api.services.storage.Storage;
import com.google.cloud.dlp.v2.DlpServiceClient;
import com.google.privacy.dlp.v2.ContentItem;
import com.google.privacy.dlp.v2.DeidentifyContentRequest;
import com.google.privacy.dlp.v2.DeidentifyContentResponse;
import com.google.privacy.dlp.v2.ProjectName;
import com.google.swarm.tokenization.common.KMSFactory;
import com.google.swarm.tokenization.common.StorageFactory;
import com.google.swarm.tokenization.common.TextStreamingPipelineOptions;
import com.google.swarm.tokenization.common.WriteOneFilePerWindow;

public class TextStreamingPipeline {

	public static final Logger LOG = LoggerFactory
			.getLogger(TextStreamingPipeline.class);

	public static void main(String[] args)
			throws IOException, GeneralSecurityException {
		TextStreamingPipelineOptions options = PipelineOptionsFactory
				.fromArgs(args).withValidation()
				.as(TextStreamingPipelineOptions.class);

		Pipeline p = Pipeline.create(options);
		p.apply(FileIO.match().filepattern(options.getInputFile()).continuously(
				Duration.standardSeconds(10), Watch.Growth.never()))
				.apply(FileIO.readMatches()
						.withCompression(Compression.UNCOMPRESSED))
				.apply("Text File Reader",
						ParDo.of(new TextFileReader(options.getDlpProject(),
								options.getFileDecryptKeyName(),
								options.getFileDecryptKey(),
								options.getBatchSize(), options.getCsek(),
								options.getCsekhash())))
				.apply("Tokenize Data",
						ParDo.of(new TokenizeData(options.getDlpProject(),
								options.getDeidentifyTemplateName(),
								options.getInspectTemplateName())))
				.apply(Window.<String>into(
						FixedWindows.of(Duration.standardMinutes(1))))
				.apply(new WriteOneFilePerWindow(options.getOutputFile().get(),
						1));

		p.run().waitUntilFinish();
	}

	@SuppressWarnings("serial")
	public static class TokenizeData extends DoFn<String, String> {

		final ValueProvider<String> projectId;
		final ValueProvider<String> deIdentifyTemplateName;
		final ValueProvider<String> inspectTemplateName;

		public TokenizeData(ValueProvider<String> projectId,
				ValueProvider<String> deIdentifyTemplateName,
				ValueProvider<String> inspectTemplateName) {
			this.projectId = projectId;
			this.deIdentifyTemplateName = deIdentifyTemplateName;
			this.inspectTemplateName = inspectTemplateName;
		}

		@ProcessElement
		public void processElement(ProcessContext c) throws IOException {

			try (DlpServiceClient dlpServiceClient = DlpServiceClient
					.create()) {

				ContentItem contentItem = ContentItem.newBuilder()
						.setValue(c.element()).build();

				DeidentifyContentRequest request = DeidentifyContentRequest
						.newBuilder()
						.setParent(ProjectName
								.of(this.projectId.get().toString()).toString())
						.setDeidentifyTemplateName(
								this.deIdentifyTemplateName.get().toString())
						.setInspectTemplateName(
								this.inspectTemplateName.get().toString())
						.setItem(contentItem).build();

				DeidentifyContentResponse response = dlpServiceClient
						.deidentifyContent(request);

				String encryptedData = response.getItem().getValue();
				LOG.info("Successfully tokenized request size: "
						+ request.toByteString().size() + " bytes");
				c.output(encryptedData);

			}

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

	private static InputStream downloadObject(Storage storage,
			String bucketName, String objectName, String base64CseKey,
			String base64CseKeyHash) throws Exception {

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

	@SuppressWarnings("serial")
	public static class TextFileReader extends DoFn<ReadableFile, String> {
		private ValueProvider<Integer> batchSize;
		private ValueProvider<String> cSek;
		private ValueProvider<String> cSekhash;
		private String objectName;
		private String bucketName;
		private String key;

		public TextFileReader(ValueProvider<String> kmsKeyProjectName,
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

			if (cSek.get().toString().equals("null")
					|| cSekhash.get().toString().equals("null")) {

				// default FileIO can handle the request as file is encrypted
				// using customer
				// managed key in KMS

				try {

					SeekableByteChannel channel = c.element().openSeekable();
					ByteBuffer bf = ByteBuffer
							.allocate(batchSize.get().intValue());
					while ((channel.read(bf)) > 0) {
						bf.flip();
						byte[] data = bf.array();
						bf.clear();
						c.output(new String(data, StandardCharsets.UTF_8));

					}
				} catch (IOException e) {
					LOG.error("Error Reading the File " + e.getMessage());
					e.printStackTrace();
					System.exit(1);

				}

			} else {

				// let input stream handle the request as file is encrypted
				// using customer
				// supplied key

				try {

					Storage storage = StorageFactory.getService();
					InputStream objectData = downloadObject(storage, bucketName,
							objectName, key, cSekhash.get().toString());

					byte[] data = new byte[this.batchSize.get().intValue()];
					int bytesRead = 0;
					int offset = 0;
					while ((bytesRead = objectData.read(data, offset,
							data.length - offset)) != -1) {
						offset += bytesRead;
						if (offset >= data.length) {

							String encryptedData = new String(data, 0, offset,
									"UTF-8");
							c.output(encryptedData);
							offset = 0;

						}
					}

					objectData.close();

				} catch (Exception e) {
					LOG.error("Error Reading the File " + e.getMessage());
					e.printStackTrace();
					System.exit(1);
				}

			}

		}
	}

}
