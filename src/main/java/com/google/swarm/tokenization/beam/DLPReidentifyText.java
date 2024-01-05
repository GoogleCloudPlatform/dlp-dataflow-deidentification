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
 * inspecting text for identifying data according to provided settings.
 *
 * <p>The transform supports both delimited columnar input data and unstructured input.
 *
 * <p>If the headerColumns property is set and a sideinput with headers is added to the PTransform,
 * delimiter also should be set, else the results will be incorrect. If headerColumns is neither set
 * nor passed as sideinput, input is assumed to be unstructured.
 *
 * <p>Batch size defines how big are batches sent to DLP at once in bytes.
 *
 * <p>The transform consumes {@link KV} of {@link String}s (assumed to be filename as key and
 * contents as value) and outputs {@link KV} of {@link String} (eg. filename) and {@link
 * ReidentifyContentResponse}, which will contain {@link Table} of results for the user to consume.
 *
 * <p>Batch size defines how big are batches sent to DLP at once in bytes.
 *
 * <p>Either reidentifyTemplateName {@link String} or reidentifyConfig {@link DeidentifyConfig} need
 * to be set. inspectConfig {@link InspectConfig} and inspectTemplateName {@link String} are
 * optional.
 *
 * <p>Batch size defines how big are batches sent to DLP at once in bytes.
 */
@Experimental
@AutoValue
public abstract class DLPReidentifyText
    extends PTransform<
        PCollection<KV<String, Table.Row>>, PCollection<KV<String, ReidentifyContentResponse>>> {

  public static final Integer DLP_PAYLOAD_LIMIT_BYTES = 524000;

  /**
   * @return Template name for data inspection.
   */
  @Nullable
  public abstract String getInspectTemplateName();

  /**
   * @return Template name for data reidentification.
   */
  @Nullable
  public abstract String getReidentifyTemplateName();

  /**
   * @return Configuration object for data inspection. If present, supersedes the template settings.
   */
  @Nullable
  public abstract InspectConfig getInspectConfig();

  /**
   * @return Configuration object for reidentification. If present, supersedes the template.
   */
  @Nullable
  public abstract DeidentifyConfig getReidentifyConfig();

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

  /**
   * @return Size of input elements batch to be sent to Cloud DLP service in one request.
   */
  public abstract Integer getBatchSizeBytes();

  /**
   * @return ID of Google Cloud project to be used when deidentifying data.
   */
  public abstract String getProjectId();

  public abstract Integer getDlpApiRetryCount();

  public abstract Integer getInitialBackoff();

  @AutoValue.Builder
  public abstract static class Builder {

    /**
     * @param inspectTemplateName Template name for data inspection.
     */
    public abstract DLPReidentifyText.Builder setInspectTemplateName(String inspectTemplateName);

    /**
     * @param inspectConfig Configuration object for data inspection. If present, supersedes the
     *     template settings.
     */
    public abstract DLPReidentifyText.Builder setInspectConfig(InspectConfig inspectConfig);

    /**
     * @param reidentifyConfig Configuration object for data deidentification. If present,
     *     supersedes the template settings.
     */
    public abstract DLPReidentifyText.Builder setReidentifyConfig(
        DeidentifyConfig reidentifyConfig);

    /**
     * @param reidentifyTemplateName Template name for data deidentification.
     */
    public abstract DLPReidentifyText.Builder setReidentifyTemplateName(
        String reidentifyTemplateName);

    /**
     * @param batchSize Size of input elements batch to be sent to Cloud DLP service in one request.
     */
    public abstract DLPReidentifyText.Builder setBatchSizeBytes(Integer batchSize);

    /**
     * @param headerColumns List of column names if the input KV value is a delimited row in a map
     *     keyed by table references.
     */
    public abstract DLPReidentifyText.Builder setHeaderColumns(
        PCollectionView<Map<String, List<String>>> headerColumns);

    /**
     * @param delimiter Delimiter to be used when splitting values from input strings into columns.
     */
    public abstract DLPReidentifyText.Builder setColumnDelimiter(Character delimiter);

    /**
     * @param projectId ID of Google Cloud project to be used when deidentifying data.
     */
    public abstract DLPReidentifyText.Builder setProjectId(String projectId);

    public abstract DLPReidentifyText.Builder setDlpApiRetryCount(Integer dlpApiRetryCount);

    public abstract DLPReidentifyText.Builder setInitialBackoff(Integer initialBackoff);

    abstract DLPReidentifyText autoBuild();

    public DLPReidentifyText build() {
      DLPReidentifyText dlpReidentifyText = autoBuild();
      if (dlpReidentifyText.getReidentifyConfig() == null
          && dlpReidentifyText.getReidentifyTemplateName() == null) {
        throw new IllegalArgumentException(
            "Either reidentifyConfig or reidentifyTemplateName need to be set!");
      }
      if (dlpReidentifyText.getBatchSizeBytes() > DLP_PAYLOAD_LIMIT_BYTES) {
        throw new IllegalArgumentException(
            String.format(
                "Batch size is too large! It should be smaller or equal than %d.",
                DLP_PAYLOAD_LIMIT_BYTES));
      }
      if (dlpReidentifyText.getColumnDelimiter() == null
          && dlpReidentifyText.getHeaderColumns() != null) {
        throw new IllegalArgumentException(
            "Column delimiter should be set if headers are present.");
      }
      if (dlpReidentifyText.getHeaderColumns() == null
          && dlpReidentifyText.getColumnDelimiter() != null) {
        throw new IllegalArgumentException(
            "Column headers should be supplied when delimiter is present.");
      }
      return dlpReidentifyText;
    }
  }

  public static DLPReidentifyText.Builder newBuilder() {
    return new AutoValue_DLPReidentifyText.Builder();
  }

  /**
   * The transform converts the contents of input PCollection into {@link Table.Row}s and then calls
   * Cloud DLP service to perform the reidentification according to provided settings.
   *
   * @param input input PCollection
   * @return PCollection after transformations
   */
  @Override
  public PCollection<KV<String, ReidentifyContentResponse>> expand(
      PCollection<KV<String, Table.Row>> input) {
    return input
        .apply("Shard Contents", new ShardRows())
        .apply("Batch Contents", ParDo.of(new BatchRequestForDLP(getBatchSizeBytes())))
        .apply("Unshard Contents", ParDo.of(new UnshardRows()))
        .apply(
            "DLPReidentify",
            ParDo.of(
                    new DLPReidentifyText.ReidentifyText(
                        getProjectId(),
                        getInspectTemplateName(),
                        getReidentifyTemplateName(),
                        getInspectConfig(),
                        getReidentifyConfig(),
                        getHeaderColumns(),
                        getDlpApiRetryCount(),
                        getInitialBackoff()))
                .withSideInputs(getHeaderColumns()));
  }

  /** Performs the calls to Cloud DLP service on GCP. */
  static class ReidentifyText
      extends DoFn<KV<String, Iterable<Table.Row>>, KV<String, ReidentifyContentResponse>> {

    public static final Logger LOG =
        LoggerFactory.getLogger(DLPReidentifyText.ReidentifyText.class);
    private final String projectId;
    private final String inspectTemplateName;
    private final String reidentifyTemplateName;
    private final InspectConfig inspectConfig;
    private final DeidentifyConfig reidentifyConfig;
    private final PCollectionView<Map<String, List<String>>> headerColumns;
    private transient ReidentifyContentRequest.Builder requestBuilder;
    private transient DlpServiceClient dlpServiceClient;
    private final Integer dlpApiRetryCount;
    private final Integer initialBackoff;
    private transient FluentBackoff backoffBuilder;

    private final Counter numberOfDLPRowBagsFailedReid =
        Metrics.counter(DLPInspectText.InspectData.class, "numberOfDLPRowBagsFailedReid");

    private final Counter numberOfDLPRowsFailedReid =
        Metrics.counter(DLPInspectText.InspectData.class, "numberOfDLPRowsFailedReid");

    @Setup
    public void setup() throws IOException {
      requestBuilder = ReidentifyContentRequest.newBuilder().setParent(projectId);
      if (inspectTemplateName != null) {
        requestBuilder.setInspectTemplateName(inspectTemplateName);
      }
      if (inspectConfig != null) {
        requestBuilder.setInspectConfig(inspectConfig);
      }
      if (reidentifyConfig != null) {
        requestBuilder.setReidentifyConfig(reidentifyConfig);
      }
      if (reidentifyTemplateName != null) {
        requestBuilder.setReidentifyTemplateName(reidentifyTemplateName);
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

    /**
     * @param projectId ID of GCP project that should be used for deidentification.
     * @param inspectTemplateName Template name for inspection. Optional.
     * @param reidentifyTemplateName Template name for reidentification. Either this or
     *     reidentifyConfig is required.
     * @param inspectConfig Configuration object for inspection. Optional.
     * @param reidentifyConfig Reidentification config containing data transformations. Either this
     *     or reidentifyTemplateName is required.
     * @param headerColumns Header row of the table if applicable.
     */
    public ReidentifyText(
        String projectId,
        String inspectTemplateName,
        String reidentifyTemplateName,
        InspectConfig inspectConfig,
        DeidentifyConfig reidentifyConfig,
        PCollectionView<Map<String, List<String>>> headerColumns,
        Integer dlpApiRetryCount,
        Integer initialBackoff) {
      this.projectId = projectId;
      this.inspectTemplateName = inspectTemplateName;
      this.reidentifyTemplateName = reidentifyTemplateName;
      this.inspectConfig = inspectConfig;
      this.reidentifyConfig = reidentifyConfig;
      this.headerColumns = headerColumns;
      this.dlpApiRetryCount = dlpApiRetryCount;
      this.initialBackoff = initialBackoff;
    }

    @ProcessElement
    public void processElement(ProcessContext context) throws IOException, InterruptedException {
      String tableRef = context.element().getKey();

      List<FieldId> tableHeaders;
      if (headerColumns != null) {
        Map<String, List<String>> headersByTableRefMap = context.sideInput(headerColumns);
        List<String> columns = headersByTableRefMap.get(tableRef);
        if (columns == null) {
          throw new RuntimeException(
              "Unable to find "
                  + tableRef
                  + " in the map with table references "
                  + headersByTableRefMap.keySet());
        }
        tableHeaders =
            columns.stream()
                .map(header -> FieldId.newBuilder().setName(header).build())
                .collect(Collectors.toList());
      } else {
        // handle unstructured input.
        tableHeaders = new ArrayList<>();
        tableHeaders.add(FieldId.newBuilder().setName("value").build());
      }
      Table table =
          Table.newBuilder()
              .addAllHeaders(tableHeaders)
              .addAllRows(context.element().getValue())
              .build();
      ContentItem contentItem = ContentItem.newBuilder().setTable(table).build();
      this.requestBuilder.setItem(contentItem);
      BackOff backoff = backoffBuilder.backoff();
      boolean retry = true;
      while (retry) {
        try {
          ReidentifyContentResponse response =
              dlpServiceClient.reidentifyContent(requestBuilder.build());

          context.output(KV.of(tableRef, response));
          break;
        } catch (ResourceExhaustedException e) {
          retry = BackOffUtils.next(Sleeper.DEFAULT, backoff);
          if (retry) {
            LOG.warn("Error in DLP API, Retrying...");
          } else {
            numberOfDLPRowBagsFailedReid.inc();
            numberOfDLPRowsFailedReid.inc(table.getRowsCount());
            LOG.error(
                "Retried {} times unsuccessfully. Not able to reidentify some records. Exception: {}",
                this.dlpApiRetryCount,
                e.getMessage());
          }
        } catch (ApiException e) {
          LOG.error(
              "DLP API returned error. Not able to reidentify some records {}", e.getMessage());
          retry = false;
        }
      }
    }
  }
}
