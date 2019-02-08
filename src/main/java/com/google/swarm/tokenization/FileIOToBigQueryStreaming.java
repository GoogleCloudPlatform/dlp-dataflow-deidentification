/*
 * Copyright (C) 2018 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.swarm.tokenization;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.FileIO.ReadableFile;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.DynamicDestinations;
import org.apache.beam.sdk.io.gcp.bigquery.TableDestination;
import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Watch;
import org.apache.beam.sdk.transforms.splittabledofn.OffsetRangeTracker;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.ValueInSingleWindow;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.client.util.Charsets;
import com.google.api.services.bigquery.model.TableCell;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.dlp.v2.DlpServiceClient;
import com.google.privacy.dlp.v2.ContentItem;
import com.google.privacy.dlp.v2.DeidentifyContentRequest;
import com.google.privacy.dlp.v2.DeidentifyContentResponse;
import com.google.privacy.dlp.v2.FieldId;
import com.google.privacy.dlp.v2.ProjectName;
import com.google.privacy.dlp.v2.Table;
import com.google.privacy.dlp.v2.Value;

/**
 * The {@link FileIOToBigQueryStreaming} is a streaming pipeline that reads CSV
 * files from a storage location (e.g. GCS), uses DLP API to tokenize sensitive
 * information (e.g. PII Data like passport or SIN number) and at the end stores
 * tokenized data in Big Query to be used for various purposes.
 * <p><b>Pipeline Requirements</b>
 *
 * <ul>
 *   <li>DLP Templates exist (e.g. deidentifyTemplate, InspectTemplate)
 *   <li>The BigQuery Dataset exists
 * </ul>
 *
 * <p><b>Example Usage</b>
 *
 * <pre>
 * # Set the pipeline vars
 * PROJECT_ID=PROJECT ID HERE
 * BUCKET_NAME=BUCKET NAME HERE
 * PIPELINE_FOLDER=gs://${BUCKET_NAME}/dataflow/pipelines/fileio-to-bigquery-dlp
 *
 * # Set the runner
 * RUNNER=DataflowRunner
 *
 * # Build the template
 * mvn compile exec:java \
 * -Dexec.mainClass=com.google.cloud.teleport.templates.FileIOToBigQueryStreaming \
 * -Dexec.cleanupDaemonThreads=false \
 * -Dexec.args=" \
 * --project=${PROJECT_ID} \
 * --stagingLocation=${PIPELINE_FOLDER}/staging \
 * --tempLocation=${PIPELINE_FOLDER}/temp \
 * --templateLocation=${PIPELINE_FOLDER}/template \
 * --runner=${RUNNER}"
 *
 * # Execute the template
 * JOB_NAME=fileio-to-bigquery-dlp-$USER-`date +"%Y%m%d-%H%M%S%z"`
 *
 * gcloud dataflow jobs run ${JOB_NAME} \
 * --gcs-location=${PIPELINE_FOLDER}/template \
 * --zone=us-east1-d \
 * --parameters \
 * "inputFilePattern=gs://<bucketName>/<fileName>.csv, batchSize=15, deidentifyTemplateName=projects/{projectId}/deidentifyTemplates/{deIdTemplateId} 
"
 * </pre>
 */

public class FileIOToBigQueryStreaming {

	public static final Logger LOG = LoggerFactory.getLogger(FileIOToBigQueryStreaming.class);

	/**
	 * Main entry point for executing the pipeline. This will run the pipeline
	 * asynchronously. If blocking execution is required, use the
	 * {@link FileIOToBigQueryStreaming#run(TokenizePipelineOptions)} method to
	 * start the pipeline and invoke {@code result.waitUntilFinish()} on the
	 * {@link PipelineResult}
	 *
	 * @param args The command-line arguments to the pipeline.
	 */

	public static void main(String[] args) {

		TokenizePipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
				.as(TokenizePipelineOptions.class);
		run(options);

	}

	/**
	 * Runs the pipeline with the supplied options.
	 *
	 * @param options The execution parameters to the pipeline.
	 * @return The result of the pipeline execution.
	 */

	public static PipelineResult run(TokenizePipelineOptions options) {
		// Create the pipeline
		Pipeline p = Pipeline.create(options);
		/*
		 * Steps: 
		 * 	1) Continuously read from the file source in a given interval. Default
		 *     interval is set to 300 seconds. Please use --pollingInterval option to
		 *     configure desire value. 
		 *  2) Create DLP Table objects by splitting the file contents based on --batchSize 
		 *     option provided. 
		 *  3) Call DLP API with the templates provided for data tokenization.
		 *  4) Process tokenized data by converting DL Table Rows to BQ Table Rows.
		 *  5) Use BQ Dynamic destination to create table and schema and insert data.
		 * 
		 */

		PCollection<ReadableFile> csvFile = p
				// 1) Continuously read from the file source in a given interval
				.apply("Poll Input Files",FileIO.match().filepattern(options.getInputFilePattern())
						.continuously(Duration.standardSeconds(options.getPollingInterval()), Watch.Growth.never()))
				.apply("Find Pattern Match",FileIO.readMatches().withCompression(Compression.UNCOMPRESSED));

		PCollection<KV<String, TableRow>> bqDataMap = csvFile
				// 2) Create DLP Table objects by splitting the file contents based on --batchSize
				.apply("Process File Contents", ParDo.of(new CSVReader(options.getBatchSize())))
				// 3) Perform DLP tokenization by using the DLP templates provided.
				.apply("DLP-Tokenization",
						ParDo.of(new DLPTokenizationDoFn(options.as(GcpOptions.class).getProject(),
								options.getDeidentifyTemplateName(), options.getInspectTemplateName())))
				// 4) Process tokenized data by converting DL Table Rows to BQ Table Rows.
				.apply("Process Tokenized Data", ParDo.of(new TableRowProcessorDoFn()));
		
		bqDataMap.apply("Write To BQ",
				// 5) Create dynamic table and insert successfully converted records into BigQuery.
				BigQueryIO.<KV<String, TableRow>>write()
						.to(new BQDestination(options.getDatasetName(), options.as(GcpOptions.class).getProject()))
						.withFormatFunction(element -> {
							LOG.debug("BQ Row {}", element.getValue().getF());
							return element.getValue();
						}).withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
						.withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED));

		return p.run();
	}

	/**
	 * The {@link TokenizePipelineOptions} interface provides the custom execution
	 * options passed by the executor at the command-line.
	 */
	public interface TokenizePipelineOptions extends PipelineOptions {

		@Description("Polling interval used to look for new files. " + "Default is set to 300 seconds")
		@Default.Integer(300)
		Integer getPollingInterval();

		void setPollingInterval(Integer seconds);

		@Description("The file pattern to read records from (e.g. gs://bucket/file-*.csv)")
		@Required
		ValueProvider<String> getInputFilePattern();

		void setInputFilePattern(ValueProvider<String> value);

		@Description("DLP Deidentify Template to be used for API request "
				+ "(e.g.projects/{project_id}/deidentifyTemplates/{deIdTemplateId}")
		@Required
		ValueProvider<String> getDeidentifyTemplateName();

		void setDeidentifyTemplateName(ValueProvider<String> value);

		@Description("DLP Inspect Template to be used for API request "
				+ "(e.g.projects/{project_id}/inspectTemplates/{inspectTemplateId}")
		ValueProvider<String> getInspectTemplateName();

		void setInspectTemplateName(ValueProvider<String> value);

		@Description("DLP API has a limit for payload size of 524KB /api call. "
				+ "That's why dataflow process will need to chunk it. User will have to decide "
				+ "on how they would like to batch the request depending on number of rows "
				+ "and how big each row is.")
		@Required
		ValueProvider<Integer> getBatchSize();

		void setBatchSize(ValueProvider<Integer> value);

		@Description("Big Query data set must exist before the pipeline runs (e.g. pii-dataset")
		ValueProvider<String> getDatasetName();

		void setDatasetName(ValueProvider<String> value);
	}

	/**
	 * The {@link CSVReader} class uses experimental Split DoFn to split each csv
	 * file in chunks and process it in non-monolithic fashion. For example: if a
	 * CSV file has 100 rows and batch size is set to 15, then initial restrictions
	 * for the SDF will be 1 to 7 and split restriction will be {{1-2},{2-3}..{7-8}}
	 * for parallel executions.
	 * 
	 * @param ReadableFile
	 *
	 * @return KV<String, Table> where key is the file name, Table is DLP table
	 *         object
	 */
	public static class CSVReader extends DoFn<ReadableFile, KV<String, Table>> {

		private static final long serialVersionUID = 1L;
		private ValueProvider<Integer> batchSize;
		private Integer lineCount;

		public CSVReader(ValueProvider<Integer> batchSize) {
			this.batchSize = batchSize;
			lineCount = 1;

		}

		@ProcessElement
		public void processElement(ProcessContext c, OffsetRangeTracker tracker) throws IOException {
			for (long i = tracker.currentRestriction().getFrom(); tracker.tryClaim(i); ++i) {
				String fileName = c.element().getMetadata().resourceId().getFilename().toString();
				// taking out .csv extension from file name e.g fileName.csv->fileName
				String[] fileKey = fileName.split("\\.", 2);
				BufferedReader br = getReader(c.element());
				List<FieldId> headers = getHeaders(br);
				List<Table.Row> rows = new ArrayList<>();
				Table dlpTable = null;
				// finding out end of line for this restriction so that we know where to start
				// reading from
				int endOfLine = (int) (i * batchSize.get().intValue());
				int startOfLine = (endOfLine - batchSize.get().intValue());
				// skipping all the rows that's not part of this restriction
				Iterator<String> csvRows = br.lines().skip(startOfLine).iterator();
				// looping through buffered reader and creating DLP Table Rows equals to batch
				// size or less
				while (csvRows.hasNext() && lineCount <= batchSize.get().intValue()) {

					String csvRow = csvRows.next();
					rows.add(convertCsvRowToTableRow(csvRow));
					lineCount += 1;
				}
				// creating DLP table and output for next transformation
				dlpTable = Table.newBuilder().addAllHeaders(headers).addAllRows(rows).build();
				c.output(KV.of(fileKey[0], dlpTable));

				LOG.debug(
						"Current Restriction From: {}, Current Restriction To: {},"
								+ " StartofLine: {}, End Of Line {}, BatchData {}",
						tracker.currentRestriction().getFrom(), tracker.currentRestriction().getTo(), startOfLine,
						endOfLine, dlpTable.getRowsCount());

			}

		}

		@GetInitialRestriction
		public OffsetRange getInitialRestriction(ReadableFile csvFile) {

			int rowCount = 0;
			int totalSplit = 0;
			BufferedReader br = getReader(csvFile);

			if (br != null) {
				// assume first row is header
				int checkRowCount = (int) br.lines().count() - 1;
				rowCount = (checkRowCount < 1) ? 1 : checkRowCount;
				totalSplit = rowCount / batchSize.get().intValue();
				int remaining = rowCount % batchSize.get().intValue();
				if (remaining > 0) {
					totalSplit = totalSplit + 2;

				} else {
					totalSplit = totalSplit + 1;
				}

			}

			LOG.debug("Initial Restriction range from 1 to: {}", totalSplit);
			return new OffsetRange(1, totalSplit);

		}

		@SplitRestriction
		public void splitRestriction(ReadableFile csvFile, OffsetRange range, OutputReceiver<OffsetRange> out) {
			// split the initial restriction by 1
			for (final OffsetRange p : range.split(1, 1)) {
				out.output(p);

			}
		}

		@NewTracker
		public OffsetRangeTracker newTracker(OffsetRange range) {
			return new OffsetRangeTracker(new OffsetRange(range.getFrom(), range.getTo()));

		}

		private BufferedReader getReader(ReadableFile csvFile) {
			BufferedReader br = null;
			ReadableByteChannel channel = null;
			// read the file and create buffered reader

			try {
				channel = csvFile.openSeekable();

			} catch (IOException e) {
				LOG.error("Failed to Read File {}", e.getMessage());
				e.printStackTrace();
			}

			if (channel != null) {

				br = new BufferedReader(Channels.newReader(channel, Charsets.ISO_8859_1.name()));

			}

			return br;
		}

		private List<FieldId> getHeaders(BufferedReader reader) throws IOException {

			// First line in the CSV file as a header and converting to DLP Field Id
			List<FieldId> headers = Arrays.stream(reader.readLine().split(","))
					.map(header -> FieldId.newBuilder().setName(header).build()).collect(Collectors.toList());
			return headers;
		}

		private Table.Row convertCsvRowToTableRow(String row) {
			// convert from CSV row to DLP Table Row
			String[] values = row.split(",");
			Table.Row.Builder tableRowBuilder = Table.Row.newBuilder();
			for (String value : values) {
				if (value != null) {
					tableRowBuilder.addValues(Value.newBuilder().setStringValue(value).build());
				} else {
					tableRowBuilder.addValues(Value.newBuilder().setStringValue("").build());

				}
			}

			return tableRowBuilder.build();
		}

	}

	/**
	 * The {@link DLPTokenizationDoFn} class executes tokenization request by
	 * calling DLP api. It uses DLP table as a content item as CSV file contains
	 * fully structured data DLP templates (e.g. de-identify, inspect) need to exist
	 * before this pipeline runs. As response from the API is received, this DoFn
	 * loops through the output DLP table and convert DLP Table Row to BQ Table Row.
	 * 
	 * @param KV<String, TableRow> where key is used as table id and table cells
	 *        used to find header values
	 *
	 * @return TableSchema and TableDestination
	 */

	public static class DLPTokenizationDoFn extends DoFn<KV<String, Table>, KV<String, Table>> {

		private static final long serialVersionUID = 1L;
		private String projectId;
		private DlpServiceClient dlpServiceClient;
		private ValueProvider<String> deIdentifyTemplateName;
		private ValueProvider<String> inspectTemplateName;
		private boolean inspectTemplateExist;

		public DLPTokenizationDoFn(String projectId, ValueProvider<String> deIdentifyTemplateName,
				ValueProvider<String> inspectTemplateName) {
			this.projectId = projectId;
			this.dlpServiceClient = null;
			this.deIdentifyTemplateName = deIdentifyTemplateName;
			this.inspectTemplateName = inspectTemplateName;
			this.inspectTemplateExist = false;

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
			Table tokenizedData = response.getItem().getTable();
			LOG.info("Request Size Successfully Tokenized:{} rows {} bytes ", tokenizedData.getRowsList().size(),
					request.toByteString().size());
			c.output(KV.of(key, tokenizedData));

		}

	}

	/**
	 * The {@link TableRowProcessorDoFn} class process tokenized DLP tables and
	 * convert them to BigQuery Table Row
	 * 
	 * @param KV<String, Table>
	 *
	 * @return KV<String, TableRow> key is used as a tableIb in next transformation
	 */
	public static class TableRowProcessorDoFn extends DoFn<KV<String, Table>, KV<String, TableRow>> {

		private static final long serialVersionUID = 1L;

		@ProcessElement
		public void processElement(ProcessContext c) {

			Table tokenizedData = c.element().getValue();
			String[] headers = extracyTableHeader(tokenizedData);
			List<Table.Row> outputRows = tokenizedData.getRowsList();
			if (outputRows.size() > 0) {
				for (Table.Row outputRow : outputRows) {
					c.output(KV.of(c.element().getKey(), createBqRow(outputRow, headers)));
				}

			}
		}

		private static String[] extracyTableHeader(Table encryptedData) {
			List<String> outputHeaders = encryptedData.getHeadersList().stream().map(FieldId::getName)
					.collect(Collectors.toList());
			String[] headers = new String[outputHeaders.size()];

			for (int i = 0; i < headers.length; i++) {
				headers[i] = checkHeaderName(outputHeaders.get(i));

			}
			return headers;
		}

		private static String checkHeaderName(String name) {
			// some checks to make sure BQ column names don't fail e.g. special characters
			String checkedHeader = name.replaceAll("\\s", "_");
			checkedHeader = checkedHeader.replaceAll("'", "");
			checkedHeader = checkedHeader.replaceAll("/", "");
			return checkedHeader;
		}

		private static TableRow createBqRow(Table.Row tokenizedValue, String[] headers) {
			TableRow bqRow = new TableRow();
			String dlpRow = tokenizedValue.getValuesList().stream().map(value -> value.getStringValue())
					.collect(Collectors.joining(","));
			String[] values = dlpRow.split(",");
			List<TableCell> cells = new ArrayList<>();
			for (int i = 0; i < values.length; i++) {
				bqRow.set(headers[i].toString(), values[i].toString());
				// creating a list of Table Cell to be used later to create BQ Table Schema
				cells.add(new TableCell().set(headers[i].toString(), values[i].toString()));

			}
			bqRow.setF(cells);
			return bqRow;

		}
	}

	/**
	 * The {@link BQDestination} class creates BigQuery table destination and table
	 * schema based on the CSV file processed in earlier transformations. Table id
	 * is same as filename Table schema is same as file header columns.
	 *
	 * @param KV<String, TableRow> where key is used as table id and table cells
	 *        used to find header values
	 *
	 * @return TableSchema and TableDestination
	 */
	public static class BQDestination extends DynamicDestinations<KV<String, TableRow>, KV<String, TableRow>> {

		private static final long serialVersionUID = 1L;
		private ValueProvider<String> datasetName;
		private String projectId;

		public BQDestination(ValueProvider<String> datasetName, String projectId) {
			this.datasetName = datasetName;
			this.projectId = projectId;

		}

		@Override
		public KV<String, TableRow> getDestination(ValueInSingleWindow<KV<String, TableRow>> element) {
			String key = element.getValue().getKey();
			String tableName = String.format("%s:%s.%s", projectId, datasetName.get(), key);
			LOG.debug("Table Name {}", tableName);
			return KV.of(tableName, element.getValue().getValue());
		}

		@Override
		public TableDestination getTable(KV<String, TableRow> destination) {
			TableDestination dest = new TableDestination(destination.getKey(),
					"pii-tokenized output data from dataflow");
			LOG.debug("Table Destination {}", dest.getTableSpec());
			return dest;
		}

		@Override
		public TableSchema getSchema(KV<String, TableRow> destination) {

			TableRow bqRow = destination.getValue();
			TableSchema schema = new TableSchema();
			List<TableFieldSchema> fields = new ArrayList<TableFieldSchema>();
			// When TableRow is created in earlier steps, setF() was
			// used to setup TableCells so that Table Schema can be constructed

			List<TableCell> cells = bqRow.getF();

			for (int i = 0; i < cells.size(); i++) {

				Map<String, Object> object = cells.get(i);
				String header = object.keySet().iterator().next();
				// currently all BQ data types are set to String
				fields.add(new TableFieldSchema().setName(header).setType("STRING"));

			}

			schema.setFields(fields);
			return schema;
		}

	}

}
