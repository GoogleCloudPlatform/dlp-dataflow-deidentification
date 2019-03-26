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
package com.google.swarm.tokenization;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.charset.StandardCharsets;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.FileIO.ReadableFile;
import org.apache.beam.sdk.io.ReadableFileCoder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.NestedValueProvider;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.Watch;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.cloud.dlp.v2.DlpServiceClient;
import com.google.privacy.dlp.v2.ContentItem;
import com.google.privacy.dlp.v2.DeidentifyContentRequest;
import com.google.privacy.dlp.v2.DeidentifyContentRequest.Builder;

import com.google.privacy.dlp.v2.DeidentifyContentResponse;
import com.google.privacy.dlp.v2.ProjectName;
import com.google.swarm.tokenization.common.AWSOptionParser;
import com.google.swarm.tokenization.common.S3ImportOptions;
import com.google.swarm.tokenization.common.TextSink;

public class S3Import {

	public static final Logger LOG = LoggerFactory.getLogger(S3Import.class);
	private static final Duration DEFAULT_POLL_INTERVAL = Duration.standardSeconds(300);
	private static final Integer DEFAULT_BATCH_SIZE = 51200;
	private static final Duration WINDOW_INTERVAL = Duration.standardSeconds(60);

	public static void main(String[] args) {
		S3ImportOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(S3ImportOptions.class);

		AWSOptionParser.formatOptions(options);

		Pipeline p = Pipeline.create(options);
		PCollection<KV<String, ReadableFile>> files = p
				.apply("Poll Input Files",
						FileIO.match().filepattern(options.getBucketUrl()).continuously(DEFAULT_POLL_INTERVAL,
								Watch.Growth.never()))
				.apply("Find Pattern Match", FileIO.readMatches().withCompression(Compression.AUTO))
				.apply(WithKeys.of(file -> file.getMetadata().resourceId().getFilename().toString()))
				.setCoder(KvCoder.of(StringUtf8Coder.of(), ReadableFileCoder.of()));

		files.apply(ParDo.of(new S3FileReader(NestedValueProvider.of(options.getBatchSize(), batchSize -> {
			if (batchSize != null) {
				return batchSize;
			} else {
				return DEFAULT_BATCH_SIZE;

			}
		})))).apply(ParDo.of(new TokenizeData(options.getProject(), options.getDeidentifyTemplateName(),
				options.getInspectTemplateName())))
				.apply("30 sec window",
						Window.<KV<String, String>>into(FixedWindows.of(WINDOW_INTERVAL))
								.triggering(AfterProcessingTime.pastFirstElementInPane().plusDelayOf(Duration.ZERO))
								.discardingFiredPanes().withAllowedLateness(Duration.ZERO))
				.apply(GroupByKey.create())
				.apply("WriteToGCS", FileIO.<String, KV<String, Iterable<String>>>writeDynamic()
						.by((SerializableFunction<KV<String, Iterable<String>>, String>) contents -> {
							return contents.getKey();
						}).via(new TextSink()).to(options.getOutputFile()).withDestinationCoder(StringUtf8Coder.of())
						.withNumShards(1).withNaming(key -> FileIO.Write.defaultNaming(key, ".txt")));

		p.run();

	}

	@SuppressWarnings("serial")
	public static class S3FileReader extends DoFn<KV<String, ReadableFile>, KV<String, String>> {

		private ValueProvider<Integer> batchSize;

		public S3FileReader(ValueProvider<Integer> batchSize) {

			this.batchSize=batchSize;

		}
		
		
		
		@ProcessElement
		public void processElement(ProcessContext c) throws IOException {

			String fileName = c.element().getKey();
			
			try (SeekableByteChannel channel = getReader(c.element().getValue())) {

				ByteBuffer bf = ByteBuffer.allocate(this.batchSize.get());
				while ((channel.read(bf)) > 0) {
					bf.flip();
					byte[] data = bf.array();
					bf.clear();
					c.output(KV.of(fileName, new String(data, StandardCharsets.UTF_8).trim()));
				}
			}

		}

	}

	@SuppressWarnings("serial")
	public static class TokenizeData extends DoFn<KV<String, String>, KV<String, String>> {
		private String projectId;
		private DlpServiceClient dlpServiceClient;
		private ValueProvider<String> deIdentifyTemplateName;
		private ValueProvider<String> inspectTemplateName;
		private boolean inspectTemplateExist;

		private Builder requestBuilder;

		public TokenizeData(String projectId, ValueProvider<String> deIdentifyTemplateName,
				ValueProvider<String> inspectTemplateName) {
			this.projectId = projectId;
			this.dlpServiceClient = null;
			this.deIdentifyTemplateName = deIdentifyTemplateName;
			this.inspectTemplateName = inspectTemplateName;
			this.inspectTemplateExist = false;

		}

		@Setup
		public void setup() {
			if (this.inspectTemplateName.isAccessible()) {
				if (this.inspectTemplateName.get() != null) {
					this.inspectTemplateExist = true;
				}
			}
			if (this.deIdentifyTemplateName.isAccessible()) {
				if (this.deIdentifyTemplateName.get() != null) {
					this.requestBuilder = DeidentifyContentRequest.newBuilder()
							.setParent(ProjectName.of(this.projectId).toString())
							.setDeidentifyTemplateName(this.deIdentifyTemplateName.get());
					if (this.inspectTemplateExist) {
						this.requestBuilder.setInspectTemplateName(this.inspectTemplateName.get());
					}
				}
			}
		}

		@StartBundle
		public void startBundle() {

			try {
				this.dlpServiceClient = DlpServiceClient.create();

			} catch (IOException e) {
				LOG.error("Failed to create DLP Service Client", e.getMessage());
				throw new RuntimeException(e);
			}
		}

		@FinishBundle
		public void finishBundle() throws Exception {
			if (this.dlpServiceClient != null) {
				this.dlpServiceClient.close();
			}
		}

		@ProcessElement
		public void processElement(ProcessContext c) throws IOException {

			ContentItem contentItem = ContentItem.newBuilder().setValue(c.element().getValue()).build();
			this.requestBuilder.setItem(contentItem);
			DeidentifyContentResponse response = dlpServiceClient.deidentifyContent(this.requestBuilder.build());

			String encryptedData = response.getItem().getValue();
			LOG.info("Successfully tokenized request size {} bytes", response.getSerializedSize());
			c.output(KV.of(c.element().getKey(), encryptedData));

		}
	}

	private static SeekableByteChannel getReader(ReadableFile eventFile) {
		SeekableByteChannel channel = null;
		try {
			channel = eventFile.openSeekable();

		} catch (IOException e) {
			LOG.error("Failed to Open File {}", e.getMessage());
			throw new RuntimeException(e);
		}
		return channel;

	}

}
