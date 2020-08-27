/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import static com.google.common.base.MoreObjects.firstNonNull;
import com.google.privacy.dlp.v2.Table;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.state.*;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Batches input rows to reduce number of requests sent to Cloud DLP service.
 */
@Experimental
public class BatchRequestForDLP extends DoFn<KV<String, Table.Row>, KV<String, Iterable<Table.Row>>> {

  public static final Logger LOG = LoggerFactory.getLogger(BatchRequestForDLP.class);

  private final Counter numberOfBatchedDLPRows =
      Metrics.counter(BatchRequestForDLP.class, "numberOfBatchedDLPRows");

  private final Counter numberOfDLPRowBatches =
      Metrics.counter(BatchRequestForDLP.class, "numberOfDLPRowBatches");

  private final Integer batchSizeLimit;

  @StateId("key")
  private final StateSpec<ValueState<String>> keySpec = StateSpecs.value();

  @StateId("batchedRows")
  private final StateSpec<BagState<Table.Row>> batchedRowsSpec = StateSpecs.bag();

  @StateId("batchSize")
  private final StateSpec<ValueState<Integer>> batchSizeSpec = StateSpecs.value();

  @TimerId("expiry")
  private final TimerSpec expirySpec = TimerSpecs.timer(TimeDomain.EVENT_TIME);

  /**
   * Constructs the batching DoFn.
   *
   * @param batchSize Desired batch size in bytes.
   */
  public BatchRequestForDLP(Integer batchSize) {
    this.batchSizeLimit = batchSize;
  }

  /**
   * Outputs batches of DLP table rows, where each batch's size fits within the specified limit.
   */
  @ProcessElement
  public void process(
      ProcessContext context,
      @StateId("key") ValueState<String> key,
      @StateId("batchedRows") BagState<Table.Row> batchedRows,
      @StateId("batchSize") ValueState<Integer> batchSize,
      @TimerId("expiry") Timer expiry,
      BoundedWindow window) {
    if (key.read() == null) {
      // Save the key so it can be retrieved in the onExpiry()
      // method when the window expires
      key.write(context.element().getKey());
      expiry.set(window.maxTimestamp());
    }

    int currentBatchSize = firstNonNull(batchSize.read(), 0);
    int rowSize = context.element().getValue().getSerializedSize();

    // Check if the row would put us over the batch size limit
    if (currentBatchSize + rowSize > batchSizeLimit) {
      // Output all rows that had been batched so far
      context.output(KV.of(context.element().getKey(), batchedRows.read()));
      numberOfDLPRowBatches.inc();
      // Reset the batch
      batchedRows.clear();
      batchSize.clear();
      currentBatchSize = 0;
    }

    // Add current row to the batch
    batchedRows.add(context.element().getValue());
    batchSize.write(currentBatchSize + rowSize);
    numberOfBatchedDLPRows.inc();
  }


  /**
   * Outputs any remaining rows when the window expires.
   */
  @OnTimer("expiry")
  public void onExpiry(
      OnTimerContext context,
      @StateId("key") ValueState<String> key,
      @StateId("batchedRows") BagState<Table.Row> batchedRows,
      @StateId("batchSize") ValueState<Integer> batchSize) {
    boolean isEmpty = firstNonNull(batchedRows.isEmpty().read(), true);
    if (!isEmpty) {
      context.output(KV.of(key.read(), batchedRows.read()));
      numberOfDLPRowBatches.inc();
      batchedRows.clear();
      batchSize.clear();
    }
  }
}
