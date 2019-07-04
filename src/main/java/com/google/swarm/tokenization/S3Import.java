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
import java.nio.channels.SeekableByteChannel;
import java.util.List;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.FileIO.ReadableFile;
import org.apache.beam.sdk.io.ReadableFileCoder;
import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.Watch;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.transforms.splittabledofn.OffsetRangeTracker;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.transforms.windowing.AfterPane;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.cloud.dlp.v2.DlpServiceClient;
import com.google.common.collect.ImmutableList;
import com.google.privacy.dlp.v2.ContentItem;
import com.google.privacy.dlp.v2.DeidentifyContentRequest;
import com.google.privacy.dlp.v2.DeidentifyContentRequest.Builder;
import com.google.privacy.dlp.v2.DeidentifyContentResponse;
import com.google.privacy.dlp.v2.ProjectName;
import com.google.swarm.tokenization.common.AWSOptionParser;
import com.google.swarm.tokenization.common.S3ImportOptions;
import com.google.swarm.tokenization.common.TextBasedReader;
import com.google.swarm.tokenization.common.TextSink;

public class S3Import {

	public static final Logger LOG = LoggerFactory.getLogger(S3Import.class);
	private static final Duration DEFAULT_POLL_INTERVAL = Duration.standardDays(1);
	public static Integer BATCH_SIZE = 524288;
	private static final Duration WINDOW_INTERVAL = Duration.standardSeconds(30);

	public static TupleTag<KV<String, String>> textReaderSuccessElements = new TupleTag<KV<String, String>>() {
	};
	public static TupleTag<String> textReaderFailedElements = new TupleTag<String>() {
	};

	public static TupleTag<KV<String, String>> apiResponseSuccessElements = new TupleTag<KV<String, String>>() {
	};
	public static TupleTag<String> apiResponseFailedElements = new TupleTag<String>() {
	};

	public static void main(String[] args) {
		S3ImportOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(S3ImportOptions.class);

		AWSOptionParser.formatOptions(options);

		Pipeline p = Pipeline.create(options);
		// s3
		PCollection<KV<String, ReadableFile>> s3Files = p
				.apply("Poll S3 Files",
						FileIO.match().filepattern(options.getS3BucketUrl()).continuously(DEFAULT_POLL_INTERVAL,
								Watch.Growth.never()))
				.apply("S3 File Match", FileIO.readMatches().withCompression(Compression.AUTO))
				.apply("Add S3 File Name as Key",
						WithKeys.of(file -> file.getMetadata().resourceId().getFilename().toString()))
				.setCoder(KvCoder.of(StringUtf8Coder.of(), ReadableFileCoder.of()));

		// gcs
		PCollection<KV<String, ReadableFile>> gcsFiles = p
				.apply("Poll GCS Files",
						FileIO.match().filepattern(options.getGcsBucketUrl()).continuously(DEFAULT_POLL_INTERVAL,
								Watch.Growth.never()))
				.apply("GCS File Match", FileIO.readMatches().withCompression(Compression.AUTO))
				.apply("Add GCS File Name As Key",
						WithKeys.of(file -> file.getMetadata().resourceId().getFilename().toString()))
				.setCoder(KvCoder.of(StringUtf8Coder.of(), ReadableFileCoder.of()));

		PCollectionList<KV<String, ReadableFile>> pcs = PCollectionList.of(s3Files).and(gcsFiles);
		PCollection<KV<String, ReadableFile>> files = pcs.apply("Combine List of Files",Flatten.<KV<String, ReadableFile>>pCollections());

		PCollectionTuple contents = files.apply("Read File Contents", ParDo.of(new TextFileReader())
				.withOutputTags(textReaderSuccessElements, TupleTagList.of(textReaderFailedElements)));

		PCollectionTuple inspectedContents = contents.get(textReaderSuccessElements).apply(
				"DLP API",
				ParDo.of(new TokenizeData(options.getProject(), options.getDeidentifyTemplateName(),
						options.getInspectTemplateName()))
						.withOutputTags(apiResponseSuccessElements, TupleTagList.of(apiResponseFailedElements)));

		inspectedContents.get(apiResponseSuccessElements).apply("Fixed Window", Window
				.<KV<String, String>>into(FixedWindows.of(WINDOW_INTERVAL))
				.triggering(AfterWatermark.pastEndOfWindow()
						.withEarlyFirings(
								AfterProcessingTime.pastFirstElementInPane().plusDelayOf(Duration.standardSeconds(10)))
						.withLateFirings(AfterPane.elementCountAtLeast(1)))
				.discardingFiredPanes().withAllowedLateness(Duration.ZERO))
				.apply("WriteToGCS", FileIO.<String, KV<String, String>>writeDynamic()
						.by((SerializableFunction<KV<String, String>, String>) contentMap -> {
							return contentMap.getKey();
						}).via(new TextSink()).to(options.getOutputFile()).withDestinationCoder(StringUtf8Coder.of())
						.withNumShards(1).withNaming(key -> FileIO.Write.defaultNaming(key, ".txt")));

		PCollectionList
		.of(ImmutableList.of(contents.get(textReaderFailedElements),
				inspectedContents.get(apiResponseFailedElements)))
		.apply("Combine Error Logs", Flatten.pCollections())
		.apply("Write Error Logs", ParDo.of(new DoFn<String, String>() {
			@ProcessElement
			public void processElement(ProcessContext c) {
				LOG.error("***ERROR*** {}", c.element().toString());
				c.output(c.element());
			}
		}));
		
		p.run();
	}

	@SuppressWarnings("serial")
	public static class TextFileReader extends DoFn<KV<String, ReadableFile>, KV<String, String>> {

		@ProcessElement
		public void processElement(ProcessContext c, RestrictionTracker<OffsetRange, Long> tracker) throws IOException {
			// create the channel
			String fileName = c.element().getKey();

			try (SeekableByteChannel channel = getReader(c.element().getValue())) {
				TextBasedReader reader = new TextBasedReader(channel, 
						tracker.currentRestriction().getFrom(),null);
			
				while (tracker.tryClaim(reader.getStartOfNextRecord())) {
					reader.readNextRecord();
					String data = reader.getCurrent();
					LOG.info("Current Restriction Range {}, Data {}", 
							tracker.currentRestriction(),data);
					c.output(textReaderSuccessElements, KV.of(fileName, data));

				}

			} catch (Exception e) {
				c.output(textReaderFailedElements, e.toString());

			}

		}

		@GetInitialRestriction
		public OffsetRange getInitialRestriction(KV<String, ReadableFile> file) throws IOException {
			long totalBytes = file.getValue().getMetadata().sizeBytes();
			LOG.info("Initial Restriction range from 0 to: {} bytes", totalBytes);
			return new OffsetRange(0, totalBytes);

		}

		@SplitRestriction
		public void splitRestriction(KV<String, ReadableFile> csvFile, OffsetRange range,
				OutputReceiver<OffsetRange> out) {

			List<OffsetRange> splits = range.split(BATCH_SIZE,1);
			LOG.info("Number of Split 1 to {}", splits.size());
			for (final OffsetRange p : splits) {
				out.output(p);
			}
		}

		@NewTracker
		public OffsetRangeTracker newTracker(OffsetRange range) {
			return new OffsetRangeTracker(new OffsetRange(range.getFrom(), range.getTo()));
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

			try {
				if (!c.element().getValue().isEmpty()) {

					ContentItem contentItem = ContentItem.newBuilder().setValue(c.element().getValue()).build();
					this.requestBuilder.setItem(contentItem);
					DeidentifyContentResponse response = dlpServiceClient
							.deidentifyContent(this.requestBuilder.build());

					String encryptedData = response.getItem().getValue();
					LOG.info("Successfully tokenized request size {} bytes for File {}", response.getSerializedSize(),
							c.element().getKey());
					c.output(apiResponseSuccessElements, KV.of(c.element().getKey(), encryptedData));
				} 

			} catch (Exception e) {
				c.output(apiResponseFailedElements, e.toString());

			}
		}
	}

	private static SeekableByteChannel getReader(ReadableFile eventFile) throws IOException {
		SeekableByteChannel channel = null;
		channel = eventFile.openSeekable();
		return channel;

	}

}