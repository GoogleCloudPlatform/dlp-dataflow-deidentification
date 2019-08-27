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
import java.util.ArrayList;
import java.util.List;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.FileIO.ReadableFile;
import org.apache.beam.sdk.io.ReadableFileCoder;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.InsertRetryPolicy;
import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.ParDo;
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
import org.joda.time.DateTimeZone;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.services.bigquery.model.TableCell;
import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.dlp.v2.DlpServiceClient;
import com.google.common.collect.ImmutableList;
import com.google.privacy.dlp.v2.ContentItem;
import com.google.privacy.dlp.v2.InspectContentRequest;
import com.google.privacy.dlp.v2.InspectContentRequest.Builder;
import com.google.privacy.dlp.v2.InspectContentResponse;
import com.google.privacy.dlp.v2.ProjectName;
import com.google.protobuf.ByteString;
import com.google.swarm.tokenization.common.AWSOptionParser;
import com.google.swarm.tokenization.common.BQDestination;
import com.google.swarm.tokenization.common.S3ImportOptions;

public class S3Import {

	private static final Logger LOG = LoggerFactory.getLogger(S3Import.class);
	private static Integer BATCH_SIZE = 520000;
	private static Integer DLP_PAYLOAD_LIMIT = 524288;
	private static final String BQ_TABLE_NAME = String.valueOf("S3_DLP_INSPECT_FINDINGS");
	private static final Duration DEFAULT_POLL_INTERVAL = Duration.standardSeconds(30);
	private static final Duration WINDOW_INTERVAL = Duration.standardSeconds(5);
	private static final DateTimeFormatter TIMESTAMP_FORMATTER = DateTimeFormat
			.forPattern("yyyy-MM-dd HH:mm:ss.SSSSSS");

	public static TupleTag<KV<String, String>> textReaderSuccessElements = new TupleTag<KV<String, String>>() {
	};
	public static TupleTag<String> textReaderFailedElements = new TupleTag<String>() {
	};

	public static TupleTag<KV<String, TableRow>> apiResponseSuccessElements = new TupleTag<KV<String, TableRow>>() {
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
		PCollection<KV<String, ReadableFile>> files = pcs
				.apply("Combine List of Files", Flatten.<KV<String, ReadableFile>>pCollections()).apply("Fixed Window",
						Window.<KV<String, ReadableFile>>into(FixedWindows.of(WINDOW_INTERVAL))
								.triggering(AfterWatermark.pastEndOfWindow()
										.withEarlyFirings(AfterProcessingTime.pastFirstElementInPane()
												.plusDelayOf(Duration.standardSeconds(10)))
										.withLateFirings(AfterPane.elementCountAtLeast(1)))
								.discardingFiredPanes().withAllowedLateness(Duration.ZERO));

		PCollectionTuple contents = files.apply("Read File Contents", ParDo.of(new TextFileReader())
				.withOutputTags(textReaderSuccessElements, TupleTagList.of(textReaderFailedElements)));

		PCollectionTuple inspectedContents = contents.get(textReaderSuccessElements).apply("DLP Inspection",
				ParDo.of(new TokenizeData(options.getProject(), options.getInspectTemplateName()))
						.withOutputTags(apiResponseSuccessElements, TupleTagList.of(apiResponseFailedElements)));

		inspectedContents.get(apiResponseSuccessElements).apply("BQ Write", BigQueryIO.<KV<String, TableRow>>write()
				.to(new BQDestination(options.getDataSetId(), options.getProject())).withFormatFunction(element -> {
					return element.getValue();
				}).withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND).withoutValidation()
				.withFailedInsertRetryPolicy(InsertRetryPolicy.retryTransientErrors())
				.withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED));

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
		public void processElement(ProcessContext c, RestrictionTracker<OffsetRange, Long> tracker) {
			// create the channel
			String fileName = c.element().getKey();
			try (SeekableByteChannel channel = getReader(c.element().getValue())) {
				ByteBuffer readBuffer = ByteBuffer.allocate(BATCH_SIZE);
				ByteString buffer = ByteString.EMPTY;
				for (long i = tracker.currentRestriction().getFrom(); tracker.tryClaim(i); ++i) {
					long startOffset = (i * BATCH_SIZE) - BATCH_SIZE;
					channel.position(startOffset);
					readBuffer = ByteBuffer.allocate(BATCH_SIZE);
					buffer = ByteString.EMPTY;
					channel.read(readBuffer);
					readBuffer.flip();
					buffer = ByteString.copyFrom(readBuffer);
					readBuffer.clear();
					LOG.debug("Current Restriction {}, Content Size{}", tracker.currentRestriction(), buffer.size());
					// String key = String.format("%d_%d_%s",
					// tracker.currentRestriction().getFrom(),
					// tracker.currentRestriction().getTo(), fileName);
					c.output(KV.of(fileName, buffer.toStringUtf8().trim()));

				}
			} catch (Exception e) {

				c.output(textReaderFailedElements, e.getMessage());

			}

		}

		@GetInitialRestriction
		public OffsetRange getInitialRestriction(KV<String, ReadableFile> file) throws IOException {
			long totalBytes = file.getValue().getMetadata().sizeBytes();
			long totalSplit = 0;
			if (totalBytes < BATCH_SIZE) {
				totalSplit = 2;
			} else {
				totalSplit = totalSplit + (totalBytes / BATCH_SIZE);
				long remaining = totalBytes % BATCH_SIZE;
				if (remaining > 0) {
					totalSplit = totalSplit + 2;

				}

			}

			LOG.debug("Total Bytes {} for File {} -Initial Restriction range from 1 to: {}", totalBytes, file.getKey(),
					totalSplit);
			return new OffsetRange(1, totalSplit);

		}

		@SplitRestriction
		public void splitRestriction(KV<String, ReadableFile> file, OffsetRange range,
				OutputReceiver<OffsetRange> out) {

			for (final OffsetRange p : range.split(1, 1)) {
				out.output(p);
			}
		}

		@NewTracker
		public OffsetRangeTracker newTracker(OffsetRange range) {
			return new OffsetRangeTracker(new OffsetRange(range.getFrom(), range.getTo()));
		}

	}

	@SuppressWarnings("serial")
	public static class TokenizeData extends DoFn<KV<String, String>, KV<String, TableRow>> {
		private String projectId;
		private ValueProvider<String> inspectTemplateName;
		private Builder requestBuilder;
		private final Counter numberOfBytesInspected = Metrics.counter(TokenizeData.class, "NumberOfBytesInspected");

		public TokenizeData(String projectId, ValueProvider<String> inspectTemplateName) {
			this.projectId = projectId;
			this.inspectTemplateName = inspectTemplateName;

		}

		@Setup
		public void setup() {
			this.requestBuilder = InspectContentRequest.newBuilder()
					.setParent(ProjectName.of(this.projectId).toString())
					.setInspectTemplateName(this.inspectTemplateName.get());
		}

		@ProcessElement
		public void processElement(ProcessContext c) throws IOException {

			try (DlpServiceClient dlpServiceClient = DlpServiceClient.create()) {
				if (!c.element().getValue().isEmpty()) {

					ContentItem contentItem = ContentItem.newBuilder().setValue(c.element().getValue()).build();
					this.requestBuilder.setItem(contentItem);

					if (this.requestBuilder.build().getSerializedSize() > DLP_PAYLOAD_LIMIT) {
						String errorMessage = String.format("Payload Size %s Exceeded Batch Size %s",
								this.requestBuilder.build().getSerializedSize(), DLP_PAYLOAD_LIMIT);
						c.output(apiResponseFailedElements, errorMessage);
					} else {

						InspectContentResponse response = dlpServiceClient.inspectContent(this.requestBuilder.build());

						String timestamp = TIMESTAMP_FORMATTER.print(Instant.now().toDateTime(DateTimeZone.UTC));

						response.getResult().getFindingsList().forEach(finding -> {

							List<TableCell> cells = new ArrayList<>();
							TableRow row = new TableRow();

							cells.add(new TableCell().set("file_name", c.element().getKey()));
							row.set("file_name", c.element().getKey());

							cells.add(new TableCell().set("inspection_timestamp", timestamp));
							row.set("inspection_timestamp", timestamp);

							cells.add(new TableCell().set("infoType", finding.getInfoType().getName()));
							row.set("infoType", finding.getInfoType().getName());

							cells.add(new TableCell().set("likelihood", finding.getLikelihood().name()));
							row.set("likelihood", finding.getLikelihood().name());

							row.setF(cells);

							c.output(apiResponseSuccessElements, KV.of(BQ_TABLE_NAME, row));

						});

						numberOfBytesInspected.inc(contentItem.getSerializedSize());
						response.findInitializationErrors().forEach(error -> {
							c.output(apiResponseFailedElements, error.toString());

						});
					}

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
