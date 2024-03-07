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
import com.google.privacy.dlp.v2.ContentItem;
import com.google.privacy.dlp.v2.DeidentifyConfig;
import com.google.privacy.dlp.v2.DeidentifyContentRequest;
import com.google.privacy.dlp.v2.DeidentifyContentResponse;
import com.google.privacy.dlp.v2.FieldId;
import com.google.privacy.dlp.v2.InspectConfig;
import com.google.privacy.dlp.v2.Table;
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
 * deidentifying text according to provided settings. The transform supports both columnar delimited
 * input data (eg. CSV) and unstructured input.
 *
 * <p>If the headerColumns property is set and a sideinput with table headers is added to the
 * PTransform, delimiter also should be set, else the results will be incorrect. If headerColumns is
 * neither set nor passed as side input, input is assumed to be unstructured.
 *
 * <p>Either deidentifyTemplateName (String) or deidentifyConfig {@link DeidentifyConfig} need to be
 * set. inspectTemplateName and inspectConfig ({@link InspectConfig} are optional.
 *
 * <p>Batch size defines how big are batches sent to DLP at once in bytes.
 *
 * <p>The transform consumes {@link KV} of {@link String}s (assumed to be filename as key and
 * contents as value) and outputs {@link KV} of {@link String} (eg. filename) and {@link
 * DeidentifyContentResponse}, which will contain {@link Table} of results for the user to consume.
 */
@Experimental
@AutoValue
public abstract class DLPDeidentifyText
    extends PTransform<
        PCollection<KV<String, Table.Row>>, PCollection<KV<String, DeidentifyContentResponse>>> {

  public static final Integer DLP_PAYLOAD_LIMIT_BYTES = 524000;

  /**
   * @return Template name for data inspection.
   */
  @Nullable
  public abstract String getInspectTemplateName();

  /**
   * @return Template name for data deidentification.
   */
  @Nullable
  public abstract String getDeidentifyTemplateName();

  /**
   * @return Configuration object for data inspection. If present, supersedes the template settings.
   */
  @Nullable
  public abstract InspectConfig getInspectConfig();

  /**
   * @return Configuration object for deidentification. If present, supersedes the template.
   */
  @Nullable
  public abstract DeidentifyConfig getDeidentifyConfig();

  /**
   * @return List of column names if the input KV value is a delimited row.
   */
  @Nullable
  public abstract PCollectionView<Map<String, List<String>>> getHeaderColumns();

  /**
   * @return Delimiter to be used when splitting values from input strings into columns.
   */
  @Nullable
  public abstract Character getColumnDelimiter();

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
    public abstract DLPDeidentifyText.Builder setInspectTemplateName(String inspectTemplateName);

    /**
     * @param headerColumns List of column names if the input KV value is a delimited row.
     */
    public abstract DLPDeidentifyText.Builder setHeaderColumns(
        PCollectionView<Map<String, List<String>>> headerColumns);

    /**
     * @param delimiter Delimiter to be used when splitting values from input strings into columns.
     */
    public abstract DLPDeidentifyText.Builder setColumnDelimiter(Character delimiter);

    /**
     * @param batchSize Size of input elements batch to be sent to Cloud DLP service in one request.
     */
    public abstract DLPDeidentifyText.Builder setBatchSizeBytes(Integer batchSize);

    /**
     * @param projectId ID of Google Cloud project to be used when deidentifying data.
     */
    public abstract DLPDeidentifyText.Builder setProjectId(String projectId);

    /**
     * @param deidentifyTemplateName Template name for data deidentification.
     */
    public abstract DLPDeidentifyText.Builder setDeidentifyTemplateName(
        String deidentifyTemplateName);

    /**
     * @param inspectConfig Configuration object for data inspection. If present, supersedes the
     *     template settings.
     */
    public abstract DLPDeidentifyText.Builder setInspectConfig(InspectConfig inspectConfig);

    /**
     * @param deidentifyConfig Configuration object for data deidentification. If present,
     *     supersedes the template settings.
     */
    public abstract DLPDeidentifyText.Builder setDeidentifyConfig(
        DeidentifyConfig deidentifyConfig);

    public abstract DLPDeidentifyText.Builder setDlpApiRetryCount(Integer dlpApiRetryCount);

    public abstract DLPDeidentifyText.Builder setInitialBackoff(Integer initialBackoff);

    abstract DLPDeidentifyText autoBuild();

    public DLPDeidentifyText build() {
      DLPDeidentifyText dlpDeidentifyText = autoBuild();
      if (dlpDeidentifyText.getDeidentifyConfig() == null
          && dlpDeidentifyText.getDeidentifyTemplateName() == null) {
        throw new IllegalArgumentException(
            "Either deidentifyConfig or deidentifyTemplateName need to be set!");
      }
      if (dlpDeidentifyText.getBatchSizeBytes() > DLP_PAYLOAD_LIMIT_BYTES) {
        throw new IllegalArgumentException(
            String.format(
                "Batch size is too large! It should be smaller or equal than %d.",
                DLP_PAYLOAD_LIMIT_BYTES));
      }
      if (dlpDeidentifyText.getColumnDelimiter() == null
          && dlpDeidentifyText.getHeaderColumns() != null) {
        throw new IllegalArgumentException(
            "Column delimiter should be set if headers are present.");
      }
      if (dlpDeidentifyText.getHeaderColumns() == null
          && dlpDeidentifyText.getColumnDelimiter() != null) {
        throw new IllegalArgumentException(
            "Column headers should be supplied when delimiter is present.");
      }
      return dlpDeidentifyText;
    }
  }

  public static DLPDeidentifyText.Builder newBuilder() {
    return new AutoValue_DLPDeidentifyText.Builder();
  }

  /**
   * The transform converts the contents of input PCollection into {@link Table.Row}s and then calls
   * Cloud DLP service to perform the deidentification according to provided settings.
   *
   * @param input input PCollection
   * @return PCollection after transformations
   */
  @Override
  public PCollection<KV<String, DeidentifyContentResponse>> expand(
      PCollection<KV<String, Table.Row>> input) {

    return input
        .apply("Shard Contents", new ShardRows())
        .apply("Batch Contents", ParDo.of(new BatchRequestForDLP(getBatchSizeBytes())))
        .apply("Unshard Contents", ParDo.of(new UnshardRows()))
        .apply(
            "DLPDeidentify",
            ParDo.of(
                    new DeidentifyText(
                        getProjectId(),
                        getInspectTemplateName(),
                        getDeidentifyTemplateName(),
                        getInspectConfig(),
                        getDeidentifyConfig(),
                        getHeaderColumns(),
                        getDlpApiRetryCount(),
                        getInitialBackoff()))
                .withSideInputs(getHeaderColumns()));
  }

  /** DoFn performing calls to Cloud DLP service on GCP. */
  static class DeidentifyText
      extends DoFn<KV<String, Iterable<Table.Row>>, KV<String, DeidentifyContentResponse>> {

    public static final Logger LOG = LoggerFactory.getLogger(DeidentifyText.class);

    // Counter to track total number of times DLP Content API calls for DEID failed
    private final Counter numberOfDLPRowBagsFailedDeid =
        Metrics.counter(DeidentifyText.class, "numberOfDLPRowBagsFailedDeid");

    // Counter to track total number of rows that failed to deidentify
    private final Counter numberOfDLPRowsFailedDeid =
        Metrics.counter(DeidentifyText.class, "numberOfDLPRowsFailedDeid");

    private final String projectId;
    private final String inspectTemplateName;
    private final String deidentifyTemplateName;
    private final InspectConfig inspectConfig;
    private final DeidentifyConfig deidentifyConfig;
    private final PCollectionView<Map<String, List<String>>> headerColumns;
    private transient DeidentifyContentRequest.Builder requestBuilder;
    private transient DlpServiceClient dlpServiceClient;
    private final Integer dlpApiRetryCount;
    private final Integer initialBackoff;
    private transient FluentBackoff backoffBuilder;

    @Setup
    public void setup() throws IOException {
      requestBuilder = DeidentifyContentRequest.newBuilder().setParent(projectId);
      if (inspectTemplateName != null) {
        requestBuilder.setInspectTemplateName(inspectTemplateName);
      }
      if (inspectConfig != null) {
        requestBuilder.setInspectConfig(inspectConfig);
      }
      if (deidentifyConfig != null) {
        requestBuilder.setDeidentifyConfig(deidentifyConfig);
      }
      if (deidentifyTemplateName != null) {
        requestBuilder.setDeidentifyTemplateName(deidentifyTemplateName);
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
     * @param deidentifyTemplateName Template name for deidentification. Either this or
     *     deidentifyConfig is required.
     * @param inspectConfig Configuration object for inspection. Optional.
     * @param deidentifyConfig Deidentification config containing data transformations. Either this
     *     or deidentifyTemplateName is required.
     * @param headerColumns Header row of the table if applicable.
     */
    public DeidentifyText(
        String projectId,
        String inspectTemplateName,
        String deidentifyTemplateName,
        InspectConfig inspectConfig,
        DeidentifyConfig deidentifyConfig,
        PCollectionView<Map<String, List<String>>> headerColumns,
        Integer dlpApiRetryCount,
        Integer initialBackoff) {
      this.projectId = projectId;
      this.inspectTemplateName = inspectTemplateName;
      this.deidentifyTemplateName = deidentifyTemplateName;
      this.inspectConfig = inspectConfig;
      this.deidentifyConfig = deidentifyConfig;
      this.headerColumns = headerColumns;
      this.dlpApiRetryCount = dlpApiRetryCount;
      this.initialBackoff = initialBackoff;
    }

    @ProcessElement
    public void processElement(ProcessContext c) throws IOException, InterruptedException {
      String fileName = c.element().getKey();

      List<FieldId> dlpTableHeaders;
      if (headerColumns != null) {
        Map<String, List<String>> headerColumnMap = c.sideInput(headerColumns);
        List<String> columns = headerColumnMap.get(fileName);
        if (columns == null) {
          throw new RuntimeException(
              "Unable to find header row for fileName: "
                  + fileName
                  + ". The side input only contains header for "
                  + headerColumnMap.keySet());
        }
        dlpTableHeaders =
            columns.stream()
                .map(header -> FieldId.newBuilder().setName(header).build())
                .collect(Collectors.toList());
      } else {
        // handle unstructured input
        dlpTableHeaders = new ArrayList<>();
        dlpTableHeaders.add(FieldId.newBuilder().setName("value").build());
      }
      Table table =
          Table.newBuilder()
              .addAllHeaders(dlpTableHeaders)
              .addAllRows(c.element().getValue())
              .build();
      ContentItem contentItem = ContentItem.newBuilder().setTable(table).build();
      this.requestBuilder.setItem(contentItem);
      BackOff backoff = backoffBuilder.backoff();
      boolean retry = true;
      while (retry) {
        try {
          DeidentifyContentResponse response =
              dlpServiceClient.deidentifyContent(this.requestBuilder.build());
          c.output(KV.of(fileName, response));
          break;
        } catch (ResourceExhaustedException e) {
          retry = BackOffUtils.next(Sleeper.DEFAULT, backoff);
          if (retry) {
            LOG.warn("Error in DLP API, Retrying...");
          } else {
            numberOfDLPRowBagsFailedDeid.inc();
            numberOfDLPRowsFailedDeid.inc(table.getRowsCount());
            LOG.error(
                "Retried {} times unsuccessfully. Some records were not de-identified. Exception: {}",
                this.dlpApiRetryCount,
                e.getMessage());
          }
        } catch (ApiException e) {
          LOG.error(
              "DLP API returned error. Some records were not de-identified {}", e.getMessage());
          retry = false;
        }
      }
    }
  }
}
