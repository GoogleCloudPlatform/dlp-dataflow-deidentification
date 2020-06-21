/*
 * Copyright 2020 Google LLC
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

import com.google.api.services.bigquery.model.TableCell;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.dlp.v2.DlpServiceClient;
import com.google.common.base.Charsets;
import com.google.privacy.dlp.v2.ContentItem;
import com.google.privacy.dlp.v2.DeidentifyContentRequest;
import com.google.privacy.dlp.v2.DeidentifyContentRequest.Builder;
import com.google.privacy.dlp.v2.DeidentifyContentResponse;
import com.google.privacy.dlp.v2.FieldId;
import com.google.privacy.dlp.v2.ProjectName;
import com.google.privacy.dlp.v2.Table;
import com.google.privacy.dlp.v2.Table.Row;
import com.google.privacy.dlp.v2.Value;
import com.google.swarm.tokenization.common.FileReader;
import java.io.BufferedReader;
import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SeekableByteChannel;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.FileIO.ReadableFile;
import org.apache.beam.sdk.io.ReadableFileCoder;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.DynamicDestinations;
import org.apache.beam.sdk.io.gcp.bigquery.InsertRetryPolicy;
import org.apache.beam.sdk.io.gcp.bigquery.TableDestination;
import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.state.BagState;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.state.Timer;
import org.apache.beam.sdk.state.TimerSpec;
import org.apache.beam.sdk.state.TimerSpecs;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.Watch;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.transforms.splittabledofn.OffsetRangeTracker;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.ValueInSingleWindow;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DLPTextToBigQueryStreamingV2 {
  public static final Logger LOG = LoggerFactory.getLogger(DLPTextToBigQueryStreamingV2.class);
  /** Default interval for polling files in GCS. */
  private static final Duration DEFAULT_POLL_INTERVAL = Duration.standardSeconds(30);
  /** Expected only CSV file in GCS bucket. */
  private static final String ALLOWED_FILE_EXTENSION = String.valueOf("csv");
  /** Regular expression that matches valid BQ table IDs. */
  private static final String TABLE_REGEXP = "[-\\w$@]{1,1024}";
  /** Regular expression that matches valid BQ column name . */
  private static final String COLUMN_NAME_REGEXP = "^[A-Za-z_]+[A-Za-z_0-9]*$";
  /** Default window interval to create side inputs for header records. */
  private static final Duration WINDOW_INTERVAL = Duration.standardSeconds(10);
  /**
   * Main entry point for executing the pipeline. This will run the pipeline asynchronously. If
   * blocking execution is required, use the {@link
   * DLPTextToBigQueryStreaming#run(TokenizePipelineOptions)} method to start the pipeline and
   * invoke {@code result.waitUntilFinish()} on the {@link PipelineResult}
   *
   * @param args The command-line arguments to the pipeline.
   */
  public static void main(String[] args) {

    TokenizePipelineOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(TokenizePipelineOptions.class);
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
    PCollection<KV<String, ReadableFile>> csvFile =
        p.apply(
                "Poll Input Files",
                FileIO.match()
                    .filepattern(options.getInputFilePattern())
                    .continuously(DEFAULT_POLL_INTERVAL, Watch.Growth.never()))
            .apply("Find Pattern Match", FileIO.readMatches().withCompression(Compression.AUTO))
            .apply("Add File Name as Key", WithKeys.of(file -> getFileName(file)))
            .setCoder(KvCoder.of(StringUtf8Coder.of(), ReadableFileCoder.of()))
            .apply(
                "Fixed Window",
                Window.<KV<String, ReadableFile>>into(FixedWindows.of(WINDOW_INTERVAL))
                    .triggering(
                        Repeatedly.forever(
                            AfterProcessingTime.pastFirstElementInPane()
                                .plusDelayOf(WINDOW_INTERVAL)))
                    .discardingFiredPanes()
                    .withAllowedLateness(Duration.ZERO));

    final PCollectionView<List<KV<String, List<String>>>> headerMap =
        csvFile
            .apply(
                "Create Header Map",
                ParDo.of(
                    new DoFn<KV<String, ReadableFile>, KV<String, List<String>>>() {
                      @ProcessElement
                      public void processElement(ProcessContext c) {
                        String fileKey = c.element().getKey();

                        try (BufferedReader br = getBufferedReader(c.element().getValue())) {
                          c.output(KV.of(fileKey, getFileHeaders(br)));

                        } catch (IOException e) {
                          LOG.error("Failed to Read File {}", e.getMessage());
                          throw new RuntimeException(e);
                        }
                      }
                    }))
            .apply("View As List", View.asList());

    csvFile
        .apply("ReadFileContents", ParDo.of(new CSVReader(options.getKeyRange())))
        .apply("BatchRequests", ParDo.of(new BatchTableRequest(options.getBatchSize())))
        .apply(
            "DLP-Tokenization",
            ParDo.of(
                    new DLPTokenizationDoFn(
                        options.getDlpProjectId(),
                        options.getDeidentifyTemplateName(),
                        options.getInspectTemplateName(),
                        headerMap))
                .withSideInputs(headerMap))
        .apply("Process Tokenized Data", ParDo.of(new TableRowProcessorDoFn()))
        .apply(
            "Write To BQ",
            BigQueryIO.<KV<String, TableRow>>write()
                .to(new BQDestination(options.getDatasetName(), options.getDlpProjectId()))
                .withFormatFunction(
                    element -> {
                      return element.getValue();
                    })
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                .withoutValidation()
                .withFailedInsertRetryPolicy(InsertRetryPolicy.retryTransientErrors()));

    return p.run();
  }
  /**
   * The {@link TokenizePipelineOptions} interface provides the custom execution options passed by
   * the executor at the command-line.
   */
  public interface TokenizePipelineOptions extends DataflowPipelineOptions {

    @Description("The file pattern to read records from (e.g. gs://bucket/file-*.csv)")
    String getInputFilePattern();

    void setInputFilePattern(String value);

    @Description(
        "DLP Deidentify Template to be used for API request "
            + "(e.g.projects/{project_id}/deidentifyTemplates/{deIdTemplateId}")
    @Required
    String getDeidentifyTemplateName();

    void setDeidentifyTemplateName(String value);

    @Description(
        "DLP Inspect Template to be used for API request "
            + "(e.g.projects/{project_id}/inspectTemplates/{inspectTemplateId}")
    String getInspectTemplateName();

    void setInspectTemplateName(String value);

    @Description("300kb as default")
    @Default.Integer(300000)
    Integer getBatchSize();

    void setBatchSize(Integer value);

    @Description("Big Query data set must exist before the pipeline runs (e.g. pii-dataset")
    String getDatasetName();

    void setDatasetName(String value);

    @Description("Project id to be used for DLP Tokenization")
    String getDlpProjectId();

    void setDlpProjectId(String value);

    @Description("Key range to increase parallel processing")
    @Default.Integer(1024)
    Integer getKeyRange();

    void setKeyRange(Integer value);
  }

  static class CSVReader extends DoFn<KV<String, ReadableFile>, KV<String, Table.Row>> {

    private final Counter numberOfRowsRead = Metrics.counter(CSVReader.class, "numberOfRowsRead");
    private Integer keyRange;
    public static Integer FILE_SPLIT_BYTES_SIZE = 100000;

    public CSVReader(Integer keyRange) {
      this.keyRange = keyRange;
    }

    @ProcessElement
    public void processElement(ProcessContext c, RestrictionTracker<OffsetRange, Long> tracker)
        throws IOException {
      String fileKey = c.element().getKey();
      try (SeekableByteChannel channel = getReader(c.element().getValue())) {
        FileReader reader =
            new FileReader(channel, tracker.currentRestriction().getFrom(), "\n".getBytes());
        while (tracker.tryClaim(reader.getStartOfNextRecord())) {
          reader.readNextRecord();
          Row content = convertCsvRowToTableRow(reader.getCurrent());
          String key = String.format("%d%s%s", new Random().nextInt(keyRange), "_", fileKey);
          numberOfRowsRead.inc();
          c.output(KV.of(key, content));
        }
      }
    }

    @GetInitialRestriction
    public OffsetRange getInitialRestriction(@Element KV<String, ReadableFile> csvFile)
        throws IOException {
      long totalBytes = csvFile.getValue().getMetadata().sizeBytes();
      LOG.info("Initial Restriction range from 1 to: {}", totalBytes);
      return new OffsetRange(0, totalBytes);
    }

    @SplitRestriction
    public void splitRestriction(
        @Element KV<String, ReadableFile> csvFile,
        @Restriction OffsetRange range,
        OutputReceiver<OffsetRange> out) {
      long totalBytes = csvFile.getValue().getMetadata().sizeBytes();
      List<OffsetRange> splits = range.split(FILE_SPLIT_BYTES_SIZE, FILE_SPLIT_BYTES_SIZE);
      LOG.debug("Number of Split {} total bytes {}", splits.size(), totalBytes);
      for (final OffsetRange p : splits) {
        out.output(p);
      }
    }

    @NewTracker
    public OffsetRangeTracker newTracker(@Restriction OffsetRange range) {
      return new OffsetRangeTracker(new OffsetRange(range.getFrom(), range.getTo()));
    }

    private Table.Row convertCsvRowToTableRow(String csvRow) {
      /** convert from CSV row to DLP Table Row */
      List<String> valueList = Arrays.asList(csvRow.split("\\,"));
      Table.Row.Builder tableRowBuilder = Table.Row.newBuilder();
      valueList.forEach(
          value -> {
            if (value != null) {
              tableRowBuilder.addValues(
                  Value.newBuilder().setStringValue(value.toString()).build());
            } else {
              tableRowBuilder.addValues(Value.newBuilder().setStringValue("").build());
            }
          });

      return tableRowBuilder.build();
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

  private static List<String> getFileHeaders(BufferedReader reader) {
    List<String> headers = new ArrayList<>();
    try {
      CSVRecord csvHeader = CSVFormat.DEFAULT.parse(reader).getRecords().get(0);
      csvHeader.forEach(
          headerValue -> {
            headers.add(headerValue);
          });
    } catch (IOException e) {
      LOG.error("Failed to get csv header values}", e.getMessage());
      throw new RuntimeException(e);
    }
    return headers;
  }

  private static String checkHeaderName(String name) {
    /** some checks to make sure BQ column names don't fail e.g. special characters */
    String checkedHeader = name.replaceAll("\\s", "_");
    checkedHeader = checkedHeader.replaceAll("'", "");
    checkedHeader = checkedHeader.replaceAll("/", "");
    if (!checkedHeader.matches(COLUMN_NAME_REGEXP)) {
      throw new IllegalArgumentException("Column name can't be matched to a valid format " + name);
    }
    return checkedHeader;
  }

  private static String getFileName(ReadableFile file) {
    String csvFileName = file.getMetadata().resourceId().getFilename().toString();
    /** taking out .csv extension from file name e.g fileName.csv->fileName */
    String[] fileKey = csvFileName.split("\\.", 2);

    if (!fileKey[1].equals(ALLOWED_FILE_EXTENSION) || !fileKey[0].matches(TABLE_REGEXP)) {
      throw new RuntimeException(
          "[Filename must contain a CSV extension "
              + " BQ table name must contain only letters, numbers, or underscores ["
              + fileKey[1]
              + "], ["
              + fileKey[0]
              + "]");
    }
    /** returning file name without extension */
    return fileKey[0];
  }

  public static class BatchTableRequest
      extends DoFn<KV<String, Table.Row>, KV<String, Iterable<Table.Row>>> {

    private static final long serialVersionUID = 1L;

    private Integer batchSize;

    public BatchTableRequest(Integer batchSize) {
      this.batchSize = batchSize;
    }

    @StateId("elementsBag")
    private final StateSpec<BagState<KV<String, Table.Row>>> elementsBag = StateSpecs.bag();

    @TimerId("eventTimer")
    private final TimerSpec eventTimer = TimerSpecs.timer(TimeDomain.EVENT_TIME);

    @ProcessElement
    public void process(
        @Element KV<String, Table.Row> element,
        @StateId("elementsBag") BagState<KV<String, Table.Row>> elementsBag,
        @TimerId("eventTimer") Timer eventTimer,
        BoundedWindow w) {
      elementsBag.add(element);
      eventTimer.set(w.maxTimestamp());
    }

    @OnTimer("eventTimer")
    public void onTimer(
        @StateId("elementsBag") BagState<KV<String, Table.Row>> elementsBag,
        OutputReceiver<KV<String, Iterable<Table.Row>>> output) {
      if (elementsBag.read().iterator().hasNext()) {
        String key = elementsBag.read().iterator().next().getKey().split("\\_")[1];
        AtomicInteger bufferSize = new AtomicInteger();
        List<Table.Row> rows = new ArrayList<>();
        elementsBag
            .read()
            .forEach(
                element -> {
                  Integer elementSize = element.getValue().getSerializedSize();
                  boolean clearBuffer = bufferSize.intValue() + elementSize.intValue() > batchSize;
                  if (clearBuffer) {
                    LOG.debug("Clear Buffer {} , Key {}", bufferSize.intValue(), element.getKey());
                    output.output(KV.of(element.getKey(), rows));
                    rows.clear();
                    bufferSize.set(0);
                    rows.add(element.getValue());
                    bufferSize.getAndAdd(Integer.valueOf(element.getValue().getSerializedSize()));

                  } else {
                    rows.add(element.getValue());
                    bufferSize.getAndAdd(Integer.valueOf(element.getValue().getSerializedSize()));
                  }
                });
        if (!rows.isEmpty()) {
          LOG.debug("Remaining rows {}", rows.size());
          output.output(KV.of(key, rows));
        }
      }
    }
  }

  static class DLPTokenizationDoFn
      extends DoFn<KV<String, Iterable<Table.Row>>, KV<String, Table>> {
    private String dlpProjectId;
    private PCollectionView<List<KV<String, List<String>>>> headerMap;

    private DlpServiceClient dlpServiceClient;
    private String deIdentifyTemplateName;
    private String inspectTemplateName;
    private boolean inspectTemplateExist;
    private Builder requestBuilder;
    List<String> csvHeaders;

    private final Counter numberOfRowsTokenized =
        Metrics.counter(DLPTokenizationDoFn.class, "numberOfRowsTokenized");

    public DLPTokenizationDoFn(
        String dlpProjectId,
        String deIdentifyTemplateName,
        String inspectTemplateName,
        PCollectionView<List<KV<String, List<String>>>> headerMap) {
      this.dlpProjectId = dlpProjectId;
      this.dlpServiceClient = null;
      this.deIdentifyTemplateName = deIdentifyTemplateName;
      this.inspectTemplateName = inspectTemplateName;
      this.inspectTemplateExist = false;
      this.headerMap = headerMap;
      this.csvHeaders = new ArrayList<>();
    }

    @Setup
    public void setup() {

      if (this.inspectTemplateName != null) {
        this.inspectTemplateExist = true;
      }

      if (this.deIdentifyTemplateName != null) {
        this.requestBuilder =
            DeidentifyContentRequest.newBuilder()
                .setParent(ProjectName.of(this.dlpProjectId).toString())
                .setDeidentifyTemplateName(this.deIdentifyTemplateName);
        if (this.inspectTemplateExist) {
          this.requestBuilder.setInspectTemplateName(this.inspectTemplateName);
        }
      }
    }

    @StartBundle
    public void startBundle() throws SQLException {

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
    public void processElement(ProcessContext c) {
      String fileKey = c.element().getKey();
      csvHeaders = getHeaders(c.sideInput(headerMap), fileKey);
      if (csvHeaders != null) {
        List<FieldId> dlpTableHeaders =
            csvHeaders.stream()
                .map(header -> FieldId.newBuilder().setName(header).build())
                .collect(Collectors.toList());
        Table nonEncryptedData =
            Table.newBuilder()
                .addAllHeaders(dlpTableHeaders)
                .addAllRows(c.element().getValue())
                .build();
        ContentItem tableItem = ContentItem.newBuilder().setTable(nonEncryptedData).build();
        this.requestBuilder.setItem(tableItem);
        DeidentifyContentResponse response =
            dlpServiceClient.deidentifyContent(this.requestBuilder.build());

        Table tokenizedData = response.getItem().getTable();
        numberOfRowsTokenized.inc(tokenizedData.getRowsList().size());
        c.output(KV.of(fileKey, tokenizedData));
      } else {
        LOG.warn("Side Input Is Empty For {}. This could cause Data Loss ", fileKey);
      }
    }

    private List<String> getHeaders(List<KV<String, List<String>>> headerMap, String fileKey) {
      return headerMap.stream()
          .filter(map -> map.getKey().equalsIgnoreCase(fileKey))
          .findFirst()
          .map(e -> e.getValue())
          .orElse(null);
    }
  }

  public static class TableRowProcessorDoFn extends DoFn<KV<String, Table>, KV<String, TableRow>> {

    @ProcessElement
    public void processElement(ProcessContext c) {

      Table tokenizedData = c.element().getValue();
      List<String> headers =
          tokenizedData.getHeadersList().stream()
              .map(fid -> fid.getName())
              .collect(Collectors.toList());
      List<Table.Row> outputRows = tokenizedData.getRowsList();
      if (outputRows.size() > 0) {
        for (Table.Row outputRow : outputRows) {
          if (outputRow.getValuesCount() != headers.size()) {
            throw new IllegalArgumentException(
                "CSV file's header count must exactly match with data element count");
          }
          c.output(
              KV.of(
                  c.element().getKey(),
                  createBqRow(outputRow, headers.toArray(new String[headers.size()]))));
        }
      }
    }

    private static TableRow createBqRow(Table.Row tokenizedValue, String[] headers) {
      TableRow bqRow = new TableRow();
      AtomicInteger headerIndex = new AtomicInteger(0);
      List<TableCell> cells = new ArrayList<>();
      tokenizedValue
          .getValuesList()
          .forEach(
              value -> {
                String checkedHeaderName =
                    checkHeaderName(headers[headerIndex.getAndIncrement()].toString());
                bqRow.set(checkedHeaderName, value.getStringValue());
                cells.add(new TableCell().set(checkedHeaderName, value.getStringValue()));
              });
      bqRow.setF(cells);
      return bqRow;
    }
  }

  public static class BQDestination
      extends DynamicDestinations<KV<String, TableRow>, KV<String, TableRow>> {

    private String datasetName;
    private String projectId;

    public BQDestination(String datasetName, String projectId) {
      this.datasetName = datasetName;
      this.projectId = projectId;
    }

    @Override
    public KV<String, TableRow> getDestination(ValueInSingleWindow<KV<String, TableRow>> element) {
      String key = element.getValue().getKey();
      String tableName = String.format("%s:%s.%s", projectId, datasetName, key);
      LOG.debug("Table Name {}", tableName);
      return KV.of(tableName, element.getValue().getValue());
    }

    @Override
    public TableDestination getTable(KV<String, TableRow> destination) {
      TableDestination dest =
          new TableDestination(destination.getKey(), "pii-tokenized output data from dataflow");
      LOG.debug("Table Destination {}", dest.getTableSpec());
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
        /** currently all BQ data types are set to String */
        fields.add(new TableFieldSchema().setName(checkHeaderName(header)).setType("STRING"));
      }

      schema.setFields(fields);
      return schema;
    }
  }

  private static BufferedReader getBufferedReader(ReadableFile csvFile) {
    BufferedReader br = null;
    ReadableByteChannel channel = null;
    /** read the file and create buffered reader */
    try {
      channel = csvFile.openSeekable();

    } catch (IOException e) {
      LOG.error("Failed to Read File {}", e.getMessage());
      throw new RuntimeException(e);
    }

    if (channel != null) {
      br = new BufferedReader(Channels.newReader(channel, Charsets.ISO_8859_1.name()));
    }
    return br;
  }
}
