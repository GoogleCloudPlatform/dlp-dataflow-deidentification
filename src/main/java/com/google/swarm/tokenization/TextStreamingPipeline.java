
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
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
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

import com.google.api.services.storage.Storage;
import com.google.cloud.dlp.v2.DlpServiceClient;
import com.google.privacy.dlp.v2.ContentItem;
import com.google.privacy.dlp.v2.DeidentifyContentRequest;
import com.google.privacy.dlp.v2.DeidentifyContentResponse;
import com.google.privacy.dlp.v2.ProjectName;
import com.google.swarm.tokenization.common.KMSFactory;
import com.google.swarm.tokenization.common.StorageFactory;
import com.google.swarm.tokenization.common.TokenizePipelineOptions;
import com.google.swarm.tokenization.common.Util;
import com.google.swarm.tokenization.common.WriteOneFilePerWindow;

public class TextStreamingPipeline {

	public static final Logger LOG = LoggerFactory.getLogger(TextStreamingPipeline.class);

	public static void main(String[] args) throws IOException, GeneralSecurityException {

		TokenizePipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
				.as(TokenizePipelineOptions.class);

		Pipeline p = Pipeline.create(options);
		p.apply(FileIO.match().filepattern(options.getInputFile())
				.continuously(Duration.standardSeconds(options.getPollingInterval()), Watch.Growth.never()))
				.apply(FileIO.readMatches().withCompression(Compression.UNCOMPRESSED))
				.apply("Text File Reader",
						ParDo.of(new TextFileReader(options.as(GcpOptions.class).getProject(),
								options.getFileDecryptKeyName(), options.getFileDecryptKey(), options.getBatchSize(),
								options.getCsek(), options.getCsekhash())))
				.apply("Tokenize Data",
						ParDo.of(new TokenizeData(options.as(GcpOptions.class).getProject(),
								options.getDeidentifyTemplateName(), options.getInspectTemplateName())))
				.apply(Window.<String>into(FixedWindows.of(Duration.standardMinutes(options.getInterval()))))
				.apply(new WriteOneFilePerWindow(options.getOutputFile(), 1));

		p.run();
	}

	@SuppressWarnings("serial")
	public static class TokenizeData extends DoFn<String, String> {

		private String projectId;
		private ValueProvider<String> deIdentifyTemplateName;
		private ValueProvider<String> inspectTemplateName;

		public TokenizeData(String projectId, ValueProvider<String> deIdentifyTemplateName,
				ValueProvider<String> inspectTemplateName) {
			this.projectId = projectId;
			System.out.println("Project Id: " + projectId);
			this.deIdentifyTemplateName = deIdentifyTemplateName;
			this.inspectTemplateName = inspectTemplateName;
		}

		@ProcessElement
		public void processElement(ProcessContext c) throws IOException {

			try (DlpServiceClient dlpServiceClient = DlpServiceClient.create()) {

				ContentItem contentItem = ContentItem.newBuilder().setValue(c.element()).build();

				DeidentifyContentRequest request = DeidentifyContentRequest.newBuilder()
						.setParent(ProjectName.of(this.projectId).toString())
						.setDeidentifyTemplateName(this.deIdentifyTemplateName.get())
						.setInspectTemplateName(this.inspectTemplateName.get()).setItem(contentItem).build();

				DeidentifyContentResponse response = dlpServiceClient.deidentifyContent(request);

				String encryptedData = response.getItem().getValue();
				LOG.info("Successfully tokenized request size: " + request.toByteString().size() + " bytes");
				c.output(encryptedData);

			}

		}
	}

	@SuppressWarnings("serial")
	public static class TextFileReader extends DoFn<ReadableFile, String> {
		private ValueProvider<Integer> batchSize;
		private ValueProvider<String> cSek;
		private ValueProvider<String> cSekhash;
		private String kmsKeyProjectName;
		private String objectName;
		private String bucketName;
		private String key;
		private boolean customerSuppliedKey;
		private ValueProvider<String> fileDecryptKey;
		private ValueProvider<String> fileDecryptKeyName;

		public TextFileReader(String kmsKeyProjectName, ValueProvider<String> fileDecryptKeyRing,
				ValueProvider<String> fileDecryptKey, ValueProvider<Integer> batchSize, ValueProvider<String> cSek,
				ValueProvider<String> cSekhash) throws IOException, GeneralSecurityException {
			this.batchSize = batchSize;
			this.kmsKeyProjectName = kmsKeyProjectName;
			this.fileDecryptKey = fileDecryptKey;
			this.fileDecryptKeyName = fileDecryptKeyRing;
			this.cSek = cSek;
			this.cSekhash = cSekhash;
			this.customerSuppliedKey = false;
			this.key = null;
		}

		@ProcessElement
		public void processElement(ProcessContext c) throws IOException, GeneralSecurityException {

			if (this.cSek.isAccessible()) {

				this.customerSuppliedKey = Util.findEncryptionType(this.fileDecryptKeyName.get(),
						this.fileDecryptKey.get(), this.cSek.get(), this.cSekhash.get());
			}
			if (customerSuppliedKey)
				this.key = KMSFactory.decrypt(this.kmsKeyProjectName, "global", this.fileDecryptKeyName.get(),
						this.fileDecryptKey.get(), this.cSek.get());
			objectName = c.element().getMetadata().resourceId().getFilename().toString();
			bucketName = Util.parseBucketName(c.element().getMetadata().resourceId().getCurrentDirectory().toString());
			LOG.info("Bucket Name: " + bucketName + " File Name: " + objectName);

			if (!this.customerSuppliedKey) {

				try {

					SeekableByteChannel channel = c.element().openSeekable();
					ByteBuffer bf = ByteBuffer.allocate(batchSize.get().intValue());
					while ((channel.read(bf)) > 0) {
						bf.flip();
						byte[] data = bf.array();
						bf.clear();
						c.output(new String(data, StandardCharsets.UTF_8).trim());

					}
				} catch (IOException e) {
					LOG.error("Error Reading the File " + e.getMessage());
					e.printStackTrace();
					System.exit(1);

				}

			} else {

				try {

					Storage storage = StorageFactory.getService();
					InputStream objectData = StorageFactory.downloadObject(storage, bucketName, objectName, key,
							cSekhash.get());

					byte[] data = new byte[this.batchSize.get().intValue()];
					int bytesRead = 0;
					int offset = 0;
					while ((bytesRead = objectData.read(data, offset, data.length - offset)) != -1) {
						offset += bytesRead;
						if (offset >= data.length) {

							String encryptedData = new String(data, 0, offset, "UTF-8");
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
