
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
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.FileIO.ReadableFile;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.TableDestination;
import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.Watch;
import org.apache.beam.sdk.transforms.splittabledofn.OffsetRangeTracker;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.dlp.v2.DlpServiceClient;
import com.google.privacy.dlp.v2.ContentItem;
import com.google.privacy.dlp.v2.DeidentifyContentRequest;
import com.google.privacy.dlp.v2.DeidentifyContentResponse;
import com.google.privacy.dlp.v2.FieldId;
import com.google.privacy.dlp.v2.ProjectName;
import com.google.privacy.dlp.v2.Table;
import com.google.swarm.tokenization.common.DLPServiceFactory;
import com.google.swarm.tokenization.common.KMSFactory;
import com.google.swarm.tokenization.common.TokenizePipelineOptions;
import com.google.swarm.tokenization.common.Util;
import com.google.swarm.tokenization.common.WriteOneFilePerWindow;

public class CSVBatchPipeline {

	public static final Logger LOG = LoggerFactory
			.getLogger(CSVBatchPipeline.class);

	@SuppressWarnings("serial")
	public static class CSVFileReader extends DoFn<ReadableFile, Table> {
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
		private BufferedReader br;
		private int numberOfRows;
		private List<FieldId> headers;

		public CSVFileReader(String kmsKeyProjectName,
				ValueProvider<String> fileDecryptKeyRing,
				ValueProvider<String> fileDecryptKey,
				ValueProvider<Integer> batchSize, ValueProvider<String> cSek,
				ValueProvider<String> cSekhash)
				throws IOException, GeneralSecurityException {

			this.batchSize = batchSize;
			this.kmsKeyProjectName = kmsKeyProjectName;
			this.fileDecryptKey = fileDecryptKey;
			this.fileDecryptKeyName = fileDecryptKeyRing;
			this.cSek = cSek;
			this.cSekhash = cSekhash;
			this.customerSuppliedKey = false;
			this.key = null;
			this.br = null;
			this.numberOfRows = 0;
			headers = new ArrayList<>();

		}

		private boolean setProcessingforCurrentRestriction(
				ReadableFile currentReader)
				throws IOException, GeneralSecurityException {
			if (this.cSek.isAccessible()) {

				this.customerSuppliedKey = Util.findEncryptionType(
						this.fileDecryptKeyName.get(),
						this.fileDecryptKey.get(), this.cSek.get(),
						this.cSekhash.get());

			}

			if (customerSuppliedKey)
				this.key = KMSFactory.decrypt(this.kmsKeyProjectName, "global",
						this.fileDecryptKeyName.get(),
						this.fileDecryptKey.get(), this.cSek.get());

			this.bucketName = Util.parseBucketName(currentReader.getMetadata()
					.resourceId().getCurrentDirectory().toString());

			this.objectName = currentReader.getMetadata().resourceId()
					.getFilename().toString();

			this.br = Util.getReader(this.customerSuppliedKey, this.objectName,
					this.bucketName, currentReader, this.key, this.cSekhash);

			this.headers = Util.getHeaders(br);
			this.numberOfRows = Util.countRecords(br);
			// System.out.println("Rows: "+numberOfRows);
			br.close();
			return true;
		}

		@ProcessElement
		public void processElement(ProcessContext c, OffsetRangeTracker tracker)
				throws IOException, GeneralSecurityException {

			if (setProcessingforCurrentRestriction(c.element())) {
				for (long i = tracker.currentRestriction().getFrom(); tracker
						.tryClaim(i); ++i) {
					int endOfLine = (int) (i * this.batchSize.get()) + 1;
					int startOfLine = (endOfLine - this.batchSize.get());

					List<String> lines = new ArrayList<>();
					this.br = Util.getReader(this.customerSuppliedKey,
							this.objectName, this.bucketName, c.element(),
							this.key, this.cSekhash);
					String line = this.br.lines().skip(startOfLine).findFirst()
							.get();
					lines.add(line);

					for (int j = startOfLine + 1; j < endOfLine
							&& j < this.numberOfRows; j++) {

						lines.add(br.readLine());

					}
					Table batchData = Util.createDLPTable(headers, lines);
					if (batchData.getRowsCount() > 0) {
						LOG.info("Current Restriction From: "
								+ tracker.currentRestriction().getFrom()
								+ " Current Restriction To: "
								+ tracker.currentRestriction().getTo()
								+ " StartofLine: " + startOfLine
								+ " End of Line: " + endOfLine + " Batch Size:"
								+ batchData.getRowsCount());
						c.output(batchData);
						lines.clear();
					}
					br.close();
				}
			}

		}

		@GetInitialRestriction
		public OffsetRange getInitialRestriction(ReadableFile dataFile)
				throws IOException, GeneralSecurityException {

			int totalSplit = 1;

			if (setProcessingforCurrentRestriction(dataFile)) {
				totalSplit = this.numberOfRows / this.batchSize.get();
				if ((this.numberOfRows % this.batchSize.get()) > 0) {
					totalSplit = totalSplit + 1;
				}
				LOG.info("Initial Restriction range from 1 to: " + totalSplit);
				br.close();

			}
			return new OffsetRange(1, totalSplit + 1);

		}
		@SplitRestriction
		public void splitRestriction(ReadableFile element, OffsetRange range,
				OutputReceiver<OffsetRange> out) {
			for (final OffsetRange p : range.split(1, 1)) {
				out.output(p);

			}
		}

		@NewTracker
		public OffsetRangeTracker newTracker(OffsetRange range) {
			return new OffsetRangeTracker(
					new OffsetRange(range.getFrom(), range.getTo()));

		}

	}

	@SuppressWarnings("serial")
	public static class FormatGCSOutputData
			extends
				DoFn<KV<String, Iterable<Table>>, String> {
		@ProcessElement
		public void processElement(ProcessContext c) {

			KV<String, Iterable<Table>> outputData = c.element();
			StringBuffer bufferedWriter = new StringBuffer();

			bufferedWriter.append(outputData.getKey());

			outputData.getValue().forEach(encryptedData -> {

				List<Table.Row> outputRows = encryptedData.getRowsList();

				for (Table.Row outputRow : outputRows) {
					String row = outputRow.getValuesList().stream()
							.map(value -> value.getStringValue())
							.collect(Collectors.joining(","));
					bufferedWriter.append(row + "\n");
				}

			});

			c.output(bufferedWriter.toString().trim());

		}
	}
	@SuppressWarnings("serial")
	public static class TokenizeData extends DoFn<Table, KV<String, Table>> {

		private String projectId;
		private ValueProvider<String> deIdentifyTemplateName;
		private ValueProvider<String> inspectTemplateName;
		private boolean inspectTemplateExist;

		public TokenizeData(String projectId,
				ValueProvider<String> deIdentifyTemplateName,
				ValueProvider<String> inspectTemplateName) {

			this.projectId = projectId;
			this.deIdentifyTemplateName = deIdentifyTemplateName;
			this.inspectTemplateName = inspectTemplateName;
			this.inspectTemplateExist = false;

		}

		@ProcessElement
		public void processElement(ProcessContext c) {

			Table nonEncryptedData = c.element();
			Table encryptedData;
			if (this.inspectTemplateName.isAccessible()) {
				if (this.inspectTemplateName.get() != null)
					this.inspectTemplateExist = true;
			}

			try {
				DlpServiceClient dlpServiceClient = DLPServiceFactory
						.getService();
				ContentItem tableItem = ContentItem.newBuilder()
						.setTable(nonEncryptedData).build();
				DeidentifyContentRequest request;
				DeidentifyContentResponse response;
				if (this.inspectTemplateExist) {
					request = DeidentifyContentRequest.newBuilder()
							.setParent(
									ProjectName.of(this.projectId).toString())
							.setDeidentifyTemplateName(
									this.deIdentifyTemplateName.get())
							.setInspectTemplateName(
									this.inspectTemplateName.get())
							.setItem(tableItem).build();
				} else {
					request = DeidentifyContentRequest.newBuilder()
							.setParent(
									ProjectName.of(this.projectId).toString())
							.setDeidentifyTemplateName(
									this.deIdentifyTemplateName.get())
							.setItem(tableItem).build();

				}

				response = dlpServiceClient.deidentifyContent(request);
				encryptedData = response.getItem().getTable();
				LOG.info("Request Size Successfully Tokenized: "
						+ request.toByteString().size() + " bytes."
						+ " Number of rows tokenized: "
						+ response.getItem().getTable().getRowsCount());
				c.output(KV.of(Util.extractTableHeader(encryptedData),
						encryptedData));
			} catch (IOException e) {

				e.printStackTrace();
			} catch (GeneralSecurityException e) {

				e.printStackTrace();
			}

		}
	}

	@SuppressWarnings("serial")
	public static class BQSchemaGenerator
			extends
				DoFn<KV<String, Iterable<Table>>, KV<String, String>> {

		private ValueProvider<String> tableSpec;

		public BQSchemaGenerator(ValueProvider<String> tableSpec) {
			this.tableSpec = tableSpec;

		}

		@ProcessElement
		public void processElement(ProcessContext c) {
			Table encryptedData = c.element().getValue().iterator().next();
			List<FieldId> outputHeaderFields = encryptedData.getHeadersList();
			List<String> outputHeaders = outputHeaderFields.stream()
					.map(FieldId::getName).collect(Collectors.toList());

			String schema = Util.toJsonString((Util.getSchema(outputHeaders)));

			if (this.tableSpec.isAccessible()) {

				TableDestination destination = new TableDestination(
						this.tableSpec.get(),
						"pii-tokenized output data from dataflow");
				c.output(KV.of(destination.getTableSpec(), schema));
			}

		}

	}

	@SuppressWarnings("serial")
	public static class FormatBQOutputData
			extends
				DoFn<KV<String, Iterable<Table>>, TableRow> {

		@ProcessElement
		public void processElement(ProcessContext c) {

			Iterable<Table> dataTables = c.element().getValue();
			dataTables.forEach(encryptedData -> {
				List<FieldId> outputHeaderFields = encryptedData
						.getHeadersList();
				List<String> outputHeaders = outputHeaderFields.stream()
						.map(FieldId::getName).collect(Collectors.toList());
				TableSchema schema = Util.getSchema(outputHeaders);
				List<Table.Row> outputRows = encryptedData.getRowsList();
				for (Table.Row outputRow : outputRows) {

					String row = outputRow.getValuesList().stream()
							.map(value -> value.getStringValue())
							.collect(Collectors.joining(","));
					String[] values = row.split(",");
					TableRow bqRow = new TableRow();
					int numberOfFields = values.length;
					int i = 0;
					while (i < numberOfFields) {
						bqRow.set(schema.getFields().get(i).getName(),
								values[i]);
						i = i + 1;
					}
					// System.out.println("row: "+bqRow.toString());
					c.output(bqRow);
				}

			});

		}
	}

	public static void main(String[] args)
			throws IOException, GeneralSecurityException {

		TokenizePipelineOptions options = PipelineOptionsFactory.fromArgs(args)
				.withValidation().as(TokenizePipelineOptions.class);

		Pipeline p = Pipeline.create(options);

		PCollection<KV<String, Iterable<Table>>> outputData = p
				.apply(FileIO.match().filepattern(options.getInputFile())
						.continuously(
								Duration.standardSeconds(
										options.getPollingInterval()),
								Watch.Growth.never()))
				.apply(FileIO.readMatches()
						.withCompression(Compression.UNCOMPRESSED))
				.apply("CSV File Reader",
						ParDo.of(new CSVFileReader(
								options.as(GcpOptions.class).getProject(),
								options.getFileDecryptKeyName(),
								options.getFileDecryptKey(),
								options.getBatchSize(), options.getCsek(),
								options.getCsekhash())))
				.apply("Tokenize Data",
						ParDo.of(new TokenizeData(
								options.as(GcpOptions.class).getProject(),
								options.getDeidentifyTemplateName(),
								options.getInspectTemplateName())))

				.apply(Window.<KV<String, Table>>into(FixedWindows
						.of(Duration.standardMinutes(options.getInterval()))))

				.apply(GroupByKey.<String, Table>create());

		outputData
				.apply("Format GCS Output Data",
						ParDo.of(new FormatGCSOutputData()))
				.apply("Write to GCS",
						new WriteOneFilePerWindow(options.getOutputFile(), 1));

		final PCollectionView<Map<String, String>> schemasView = outputData
				.apply("CreateSchemaMap",
						ParDo.of(new BQSchemaGenerator(options.getTableSpec())))
				.apply("ViewSchemaAsMap", View.asMap());

		outputData
				.apply("Format BQ Output Data",
						ParDo.of(new FormatBQOutputData()))
				.apply(BigQueryIO.writeTableRows().to(options.getTableSpec())
						.withSchemaFromView(schemasView)
						.withCreateDisposition(
								BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
						.withWriteDisposition(
								BigQueryIO.Write.WriteDisposition.WRITE_APPEND));

		p.run();
	}

}