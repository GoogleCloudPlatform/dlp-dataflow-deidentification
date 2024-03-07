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
package com.google.swarm.tokenization.beam;

import com.google.api.gax.rpc.ApiException;
import com.google.api.gax.rpc.ResourceExhaustedException;
import com.google.auto.value.AutoValue;
import com.google.cloud.dlp.v2.DlpServiceClient;
import com.google.privacy.dlp.v2.*;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.util.BackOff;
import org.apache.beam.sdk.util.BackOffUtils;
import org.apache.beam.sdk.util.FluentBackoff;
import org.apache.beam.sdk.util.Sleeper;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link PTransform} connecting to Cloud DLP (https://cloud.google.com/dlp/docs/libraries) and
 * inspecting text for identifying data according to provided settings. The transform supports both
 * delimited columnar input data (eg. CSV) and unstructured input.
 *
 * <p>If the headerColumns property is set and a sideinput with table headers is added to the
 * PTransform, delimiter also should be set, else the results will be incorrect. If headerColumns is
 * neither set nor passed as sideinput, input is assumed to be unstructured.
 *
 * <p>Batch size defines how big are batches sent to DLP at once in bytes.
 *
 * <p>The transform consumes {@link KV} of {@link String}s (assumed to be filename as key and
 * contents as value) and outputs {@link KV} of {@link String} (eg. filename) and {@link
 * InspectContentResponse}, which will contain a list of {@link
 * com.google.privacy.dlp.v2.InspectResult} for the user to consume.
 *
 * <p>Either inspectTemplateName (String) or inspectConfig {@link InspectConfig} need to be set.
 *
 * <p>Batch size defines how big are batches sent to DLP at once in bytes.
 */
@Experimental
@AutoValue
public abstract class DLPInspectText
    extends PTransform<
        PCollection<KV<String, Table.Row>>, PCollection<KV<String, InspectContentResponse>>> {

  public static final Integer DLP_PAYLOAD_LIMIT_BYTES = 524000;

  /**
   * @return Template name for data inspection.
   */
  @Nullable
  public abstract String getInspectTemplateName();

  /**
   * @return Configuration object for data inspection. If present, supersedes the template settings.
   */
  @Nullable
  public abstract InspectConfig getInspectConfig();

  /**
   * @return Size of input elements batch to be sent to Cloud DLP service in one request.
   */
  public abstract Integer getBatchSizeBytes();

  /**
   * @return ID of Google Cloud project to be used when deidentifying data.
   */
  public abstract String getProjectId();

  /**
   * @return Delimiter to be used when splitting values from input strings into columns.
   */
  @Nullable
  public abstract Character getColumnDelimiter();

  /**
   * @return List of column names if the input KV value is a delimited row.
   */
  @Nullable
  public abstract PCollectionView<Map<String, List<String>>> getHeaderColumns();

  public abstract Integer getDlpApiRetryCount();

  public abstract Integer getInitialBackoff();

  @AutoValue.Builder
  public abstract static class Builder {

    /**
     * @param inspectTemplateName Template name for data inspection.
     */
    public abstract Builder setInspectTemplateName(String inspectTemplateName);

    /**
     * @param inspectConfig Configuration object for data inspection. If present, supersedes the
     *     template settings.
     */
    public abstract Builder setInspectConfig(InspectConfig inspectConfig);

    /**
     * @param batchSize Size of input elements batch to be sent to Cloud DLP service in one request.
     */
    public abstract Builder setBatchSizeBytes(Integer batchSize);

    /**
     * @param projectId ID of Google Cloud project to be used when deidentifying data.
     */
    public abstract Builder setProjectId(String projectId);

    /**
     * @param delimiter Delimiter to be used when splitting values from input strings into columns.
     */
    public abstract Builder setColumnDelimiter(Character delimiter);

    /**
     * @param headerColumns List of column names if the input KV value is a delimited row.
     */
    public abstract Builder setHeaderColumns(
        PCollectionView<Map<String, List<String>>> headerColumns);

    public abstract DLPInspectText.Builder setDlpApiRetryCount(Integer dlpApiRetryCount);

    public abstract DLPInspectText.Builder setInitialBackoff(Integer initialBackoff);

    abstract DLPInspectText autoBuild();

    public DLPInspectText build() {
      DLPInspectText inspectText = autoBuild();
      if (inspectText.getInspectTemplateName() == null && inspectText.getInspectConfig() == null) {
        throw new IllegalArgumentException(
            "Either inspectTemplateName or inspectConfig must be supplied!");
      }
      if (inspectText.getBatchSizeBytes() > DLP_PAYLOAD_LIMIT_BYTES) {
        throw new IllegalArgumentException(
            String.format(
                "Batch size is too large! It should be smaller or equal than %d.",
                DLP_PAYLOAD_LIMIT_BYTES));
      }
      if (inspectText.getColumnDelimiter() == null && inspectText.getHeaderColumns() != null) {
        throw new IllegalArgumentException(
            "Column delimiter should be set if headers are present.");
      }
      if (inspectText.getHeaderColumns() == null && inspectText.getColumnDelimiter() != null) {
        throw new IllegalArgumentException(
            "Column headers should be supplied when delimiter is present.");
      }
      return inspectText;
    }
  }

  public static Builder newBuilder() {
    return new AutoValue_DLPInspectText.Builder();
  }

  /**
   * The transform converts the contents of input PCollection into {@link Table.Row}s and then calls
   * Cloud DLP service to perform the data inspection according to provided settings.
   *
   * @param input input PCollection
   * @return PCollection after transformations
   */
  @Override
  public PCollection<KV<String, InspectContentResponse>> expand(
      PCollection<KV<String, Table.Row>> input) {
    return input
        .apply("Shard Contents", new ShardRows())
        .apply("Batch Contents", ParDo.of(new BatchRequestForDLP(getBatchSizeBytes())))
        .apply("Unshard Contents", ParDo.of(new UnshardRows()))
        .apply(
            "DLPInspect",
            ParDo.of(
                    new InspectData(
                        getProjectId(),
                        getInspectTemplateName(),
                        getInspectConfig(),
                        getHeaderColumns(),
                        getDlpApiRetryCount(),
                        getInitialBackoff()))
                .withSideInputs(getHeaderColumns()));
  }

  /** Performs calls to Cloud DLP service on GCP to inspect input data. */
  static class InspectData
      extends DoFn<KV<String, Iterable<Table.Row>>, KV<String, InspectContentResponse>> {

    public static final Logger LOG = LoggerFactory.getLogger(DLPInspectText.InspectData.class);
    private final String projectId;
    private final String inspectTemplateName;
    private final InspectConfig inspectConfig;
    private final PCollectionView<Map<String, List<String>>> headerColumns;
    private transient DlpServiceClient dlpServiceClient;
    private transient InspectContentRequest.Builder requestBuilder;
    private final Integer dlpApiRetryCount;
    private final Integer initialBackoff;
    private transient FluentBackoff backoffBuilder;

    // Counter to track total number of Rows inspected from DLP inspection
    private final Counter numberOfRowsInspected =
        Metrics.counter(InspectData.class, "numberOfRowsInspected");

    // Counter to track total number of DLP API calls made for DLP Inspection
    private final Counter numberOfDlpApiCalls =
        Metrics.counter(InspectData.class, "numberOfDlpApiCalls");

    private final Counter numberOfDLPRowBagsFailedInspection =
        Metrics.counter(DLPInspectText.InspectData.class, "numberOfDLPRowBagsFailedInspection");

    private final Counter numberOfDLPRowsFailedInspection =
        Metrics.counter(DLPInspectText.InspectData.class, "numberOfDLPRowsFailedInspection");

    /**
     * @param projectId ID of GCP project that should be used for data inspection.
     * @param inspectTemplateName Template name for inspection.
     * @param inspectConfig Configuration object for inspection.
     * @param headerColumns Header row of the table if applicable.
     * @param dlpApiRetryCount
     * @param initialBackoff
     */
    public InspectData(
        String projectId,
        String inspectTemplateName,
        InspectConfig inspectConfig,
        PCollectionView<Map<String, List<String>>> headerColumns,
        Integer dlpApiRetryCount,
        Integer initialBackoff) {
      this.projectId = projectId;
      this.inspectTemplateName = inspectTemplateName;
      this.inspectConfig = inspectConfig;
      this.headerColumns = headerColumns;
      this.dlpApiRetryCount = dlpApiRetryCount;
      this.initialBackoff = initialBackoff;
    }

    @Setup
    public void setup() throws IOException {
      this.requestBuilder = InspectContentRequest.newBuilder().setParent(projectId);
      if (inspectTemplateName != null) {
        requestBuilder.setInspectTemplateName(this.inspectTemplateName);
      }
      if (inspectConfig != null) {
        requestBuilder.setInspectConfig(inspectConfig);
      } else {
        InspectConfig config = InspectConfig.newBuilder().setIncludeQuote(true).build();
        requestBuilder.setInspectConfig(config);
      }
      dlpServiceClient = DlpServiceClient.create();
      backoffBuilder =
          FluentBackoff.DEFAULT
              .withMaxRetries(dlpApiRetryCount)
              .withInitialBackoff(Duration.standardSeconds(initialBackoff));
    }

    @Teardown
    public void teardown() {
      dlpServiceClient.close();
    }

    @ProcessElement
    public void processElement(ProcessContext c) throws IOException, InterruptedException {
      String tableRef = c.element().getKey();
      List<FieldId> tableHeaders;
      if (headerColumns != null) {
        Map<String, List<String>> headersByTableRef = c.sideInput(headerColumns);
        List<String> columns = headersByTableRef.get(tableRef);
        if (columns == null) {
          throw new RuntimeException(
              "Unable to find table reference "
                  + tableRef
                  + " in a map with keys "
                  + headersByTableRef.keySet());
        }
        tableHeaders =
            columns.stream()
                .map(header -> FieldId.newBuilder().setName(header).build())
                .collect(Collectors.toList());
      } else {
        tableHeaders = new ArrayList<>();
        tableHeaders.add(FieldId.newBuilder().setName("value").build());
      }
      Table table =
          Table.newBuilder().addAllHeaders(tableHeaders).addAllRows(c.element().getValue()).build();
      ContentItem contentItem = ContentItem.newBuilder().setTable(table).build();
      this.requestBuilder.setItem(contentItem);
      BackOff backoff = backoffBuilder.backoff();
      numberOfRowsInspected.inc(table.getRowsCount());
      boolean retry = true;
      while (retry) {
        try {
          InspectContentResponse response =
              dlpServiceClient.inspectContent(this.requestBuilder.build());

          numberOfDlpApiCalls.inc();
          c.output(KV.of(tableRef, response));
          break;
        } catch (ResourceExhaustedException e) {
          retry = BackOffUtils.next(Sleeper.DEFAULT, backoff);
          if (retry) {
            LOG.warn("Error in DLP API, Retrying...");
          } else {
            numberOfDLPRowBagsFailedInspection.inc();
            numberOfDLPRowsFailedInspection.inc(table.getRowsCount());
            LOG.error(
                "Retried {} times unsuccessfully. Not able to inspect some records. Exception: {}",
                this.dlpApiRetryCount,
                e.getMessage());
          }
        } catch (ApiException e) {
          LOG.error("DLP API returned error. Not able to inspect some records {}", e.getMessage());
          retry = false;
        }
      }
    }
  }
}
