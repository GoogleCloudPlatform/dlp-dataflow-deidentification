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

import com.google.privacy.dlp.v2.Table;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.state.*;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Batches input rows to reduce number of requests sent to Cloud DLP service. */
public class BatchRequestForDLP
    extends DoFn<KV<String, Table.Row>, KV<String, Iterable<Table.Row>>> {
  public static final Logger LOG = LoggerFactory.getLogger(BatchRequestForDLP.class);

  private final Counter numberOfDLPRowsBagged =
      Metrics.counter(BatchRequestForDLP.class, "numberOfDLPRowsBagged");
  private final Counter numberOfDLPRowBags =
      Metrics.counter(BatchRequestForDLP.class, "numberOfDLPRowBags");

  private final Integer batchSizeBytes;

  @StateId("elementsBag")
  private final StateSpec<BagState<KV<String, Table.Row>>> elementsBag = StateSpecs.bag();

  @TimerId("eventTimer")
  private final TimerSpec eventTimer = TimerSpecs.timer(TimeDomain.EVENT_TIME);

  /**
   * Constructs the batching DoFn.
   *
   * @param batchSize Desired batch size in bytes.
   */
  public BatchRequestForDLP(Integer batchSize) {
    this.batchSizeBytes = batchSize;
  }

  @ProcessElement
  public void process(
      @Element KV<String, Table.Row> element,
      @StateId("elementsBag") BagState<KV<String, Table.Row>> elementsBag,
      @TimerId("eventTimer") Timer eventTimer,
      BoundedWindow w) {
    elementsBag.add(element);
    eventTimer.set(w.maxTimestamp());
  }

  /**
   * Outputs the elements buffered in the elementsBag in batches of desired size.
   *
   * @param elementsBag element buffer.
   * @param output Batched input elements.
   */
  @OnTimer("eventTimer")
  public void onTimer(
      @StateId("elementsBag") BagState<KV<String, Table.Row>> elementsBag,
      OutputReceiver<KV<String, Iterable<Table.Row>>> output) {
    if (elementsBag.read().iterator().hasNext()) {
      String key = elementsBag.read().iterator().next().getKey();
      AtomicInteger bufferSize = new AtomicInteger();
      List<Table.Row> rows = new ArrayList<>();

      for (KV<String, Table.Row> element : elementsBag.read()) {
        int elementSize = element.getValue().getSerializedSize();
        boolean clearBuffer = bufferSize.intValue() + elementSize > batchSizeBytes;
        if (clearBuffer) {
          LOG.debug("Clear buffer of {} bytes, Key {}", bufferSize.intValue(), element.getKey());
          numberOfDLPRowsBagged.inc(rows.size());
          numberOfDLPRowBags.inc();
          output.output(KV.of(key, rows));
          rows = new ArrayList<>();
          bufferSize.set(0);
        }
        rows.add(element.getValue());
        bufferSize.getAndAdd(elementSize);
      }

      if (!rows.isEmpty()) {
        LOG.debug("Outputting remaining {} rows.", rows.size());
        numberOfDLPRowsBagged.inc(rows.size());
        numberOfDLPRowBags.inc();
        output.output(KV.of(key, rows));
      }
    }
  }
}
