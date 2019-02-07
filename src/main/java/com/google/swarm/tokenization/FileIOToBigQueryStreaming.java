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
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Watch;
import org.apache.beam.sdk.transforms.splittabledofn.OffsetRangeTracker;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
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
 * A template that copies data from a relational database using JDBC to an
 * existing BigQuery table.
 */

public class FileIOToBigQueryStreaming {

	public static final Logger LOG = LoggerFactory.getLogger(FileIOToBigQueryStreaming.class);

	/**
	 * A template that copies data from a relational database using JDBC to an
	 * existing BigQuery table.
	 */
	public interface TokenizePipelineOptions extends PipelineOptions {

		@Description("Windowed interval")
		@Default.Integer(10)
		Integer getInterval();

		void setInterval(Integer seconds);

		@Description("Pollinginterval")
		@Default.Integer(300)
		Integer getPollingInterval();

		void setPollingInterval(Integer seconds);

		@Description("Path of the file to read from")
		ValueProvider<String> getInputFile();

		void setInputFile(ValueProvider<String> value);

		@Description("Template to DeIdentiy")
		ValueProvider<String> getDeidentifyTemplateName();

		void setDeidentifyTemplateName(ValueProvider<String> value);

		@Description("Template to Inspect")
		ValueProvider<String> getInspectTemplateName();

		void setInspectTemplateName(ValueProvider<String> value);

		@Description("batch Size")
		ValueProvider<Integer> getBatchSize();

		void setBatchSize(ValueProvider<Integer> value);

		@Description("DataSet Spec")
		ValueProvider<String> getDataset();

		void setDataset(ValueProvider<String> value);
	}

	@SuppressWarnings("serial")
	public static class BQDestination extends DynamicDestinations<KV<String, TableRow>, KV<String, TableRow>> {

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
			LOG.debug("tableName {}", tableName);
			return KV.of(tableName, element.getValue().getValue());
		}

		@Override
		public TableDestination getTable(KV<String, TableRow> destination) {
			TableDestination dest = new TableDestination(destination.getKey(),
					"pii-tokenized output data from dataflow");
			LOG.debug("Table Dest {}", dest.getTableSpec());
			return dest;
		}

		@Override
		public TableSchema getSchema(KV<String, TableRow> destination) {

			TableRow bqRow = destination.getValue();
			TableSchema schema = new TableSchema();
			List<TableFieldSchema> fields = new ArrayList<TableFieldSchema>();

			List<TableCell> cells = bqRow.getF();

			for (int i = 0; i < cells.size(); i++) {

				Map<String, Object> object = cells.get(i);
				String header = object.keySet().iterator().next();
				fields.add(new TableFieldSchema().setName(header).setType("STRING"));

			}

			schema.setFields(fields);
			return schema;
		}

	}

	/**
	 * A template that copies data from a relational database using JDBC to an
	 * existing BigQuery table.
	 */
	@SuppressWarnings("serial")
	public static class DLPTokenizationDoFn extends DoFn<KV<String, Table>, KV<String, TableRow>> {

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

			String outputHeader = encryptedData.getHeadersList().stream().map(FieldId::getName)
					.collect(Collectors.joining(","));
			String[] headers = outputHeader.split(",");
			List<Table.Row> outputRows = encryptedData.getRowsList();
			if (outputRows.size() > 0) {
				for (Table.Row outputRow : outputRows) {

					TableRow bqRow = new TableRow();
					String dlpRow = outputRow.getValuesList().stream().map(value -> value.getStringValue())
							.collect(Collectors.joining(","));
					String[] values = dlpRow.split(",");
					List<TableCell> cells = new ArrayList<>();
					for (int i = 0; i < values.length; i++) {
						bqRow.set(headers[i].toString(), values[i].toString());
						cells.add(new TableCell().set(headers[i].toString(), values[i].toString()));

					}
					bqRow.setF(cells);
					
					c.output(KV.of(key, bqRow));
				}

			}

		}
	}

	/**
	 * A template that copies data from a relational database using JDBC to an
	 * existing BigQuery table.
	 */
	public static class CSVReader extends DoFn<ReadableFile, KV<String, Table>> {

		/**
		 * 
		 */
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
				String[] fileKey = fileName.split("\\.", 2);
				BufferedReader br = getReader(c.element());
				List<FieldId> headers = getHeaders(br);
				List<Table.Row> rows = new ArrayList<>();
				Table dlpTable = null;
				int endOfLine = (int) (i * batchSize.get().intValue());
				int startOfLine = (endOfLine - batchSize.get().intValue());
				Iterator<String> csvRows = br.lines().skip(startOfLine).iterator();
				while (csvRows.hasNext() && lineCount <= batchSize.get().intValue()) {

					String csvRow = csvRows.next();
					rows.add(convertCsvRowToTableRow(csvRow));
					lineCount += 1;
				}

				dlpTable = Table.newBuilder().addAllHeaders(headers).addAllRows(rows).build();
				c.output(KV.of(String.format("%s_%d", fileKey[0], i), dlpTable));

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
				rowCount = (checkRowCount<1)?1:checkRowCount;
				totalSplit = rowCount / batchSize.get().intValue();
				int remaining = rowCount % batchSize.get().intValue();
				if (remaining > 0) {
					totalSplit = totalSplit + 2;

				} else {
					totalSplit = totalSplit + 1;
				}

			}

			LOG.info("Initial Restriction range from 1 to: {}", totalSplit);
			return new OffsetRange(1, totalSplit);

		}

		@SplitRestriction
		public void splitRestriction(ReadableFile csvFile, OffsetRange range, OutputReceiver<OffsetRange> out) {
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

			List<FieldId> headers = Arrays.stream(reader.readLine().split(","))
					.map(header -> FieldId.newBuilder().setName(checkHeaderName(header)).build())
					.collect(Collectors.toList());
			return headers;
		}

		private String checkHeaderName(String name) {
			String checkedHeader = name.replaceAll("\\s", "_");
			checkedHeader = checkedHeader.replaceAll("'", "");
			checkedHeader = checkedHeader.replaceAll("/", "");
			return checkedHeader;
		}

		private Table.Row convertCsvRowToTableRow(String row) {
			String[] values = row.split(",");
			Table.Row.Builder tableRowBuilder = Table.Row.newBuilder();
			for (String value : values) {
				tableRowBuilder.addValues(Value.newBuilder().setStringValue(value).build());
			}

			return tableRowBuilder.build();
		}

	}

	public static PipelineResult run(TokenizePipelineOptions options) {
		Pipeline p = Pipeline.create(options);

		PCollection<ReadableFile> csvFile = p
				.apply(FileIO.match().filepattern(options.getInputFile())
						.continuously(Duration.standardSeconds(options.getPollingInterval()), Watch.Growth.never()))
				.apply(FileIO.readMatches().withCompression(Compression.UNCOMPRESSED));

		PCollection<KV<String, TableRow>> bqDataMap = csvFile
				.apply("Create DLP Table", ParDo.of(new CSVReader(options.getBatchSize())))
				.apply("DoDLPTokenization",
						ParDo.of(new DLPTokenizationDoFn(options.as(GcpOptions.class).getProject(),
								options.getDeidentifyTemplateName(), options.getInspectTemplateName())))
				.apply(Window
						.<KV<String, TableRow>>into(FixedWindows.of(Duration.standardSeconds(options.getInterval())))
						.triggering(
								AfterProcessingTime.pastFirstElementInPane().plusDelayOf(Duration.standardMinutes(1)))
						.discardingFiredPanes().withAllowedLateness(Duration.standardMinutes(1)));

		bqDataMap.apply("WriteToBQ",
				BigQueryIO.<KV<String, TableRow>>write()
						.to(new BQDestination(options.getDataset(), options.as(GcpOptions.class).getProject()))
						.withFormatFunction(element -> {
							LOG.debug("BQ Row {}", element.getValue().getF());
							return element.getValue();
						}).withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
						.withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED));

		return p.run();
	}

	public static void main(String[] args) {

		TokenizePipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
				.as(TokenizePipelineOptions.class);
		run(options);

	}
}
