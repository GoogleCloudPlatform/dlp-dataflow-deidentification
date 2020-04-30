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
package com.google.swarm.tokenization.common;

import org.apache.beam.sdk.schemas.transforms.Group;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AuditInspectDataTransform extends PTransform<PCollection<Row>, PCollection<Row>> {
  public static final Logger LOG = LoggerFactory.getLogger(AuditInspectDataTransform.class);

  @Override
  public PCollection<Row> expand(PCollection<Row> inspectedRow) {
    return inspectedRow
        .apply(
            "Fixed Window",
            Window.<Row>into(FixedWindows.of(Duration.standardSeconds(10)))
                .triggering(AfterWatermark.pastEndOfWindow())
                .discardingFiredPanes()
                .withAllowedLateness(Duration.ZERO))
        .apply(
            "AggrAuditData",
            Group.<Row>byFieldNames("source_file")
                .aggregateField("total_bytes_inspected", Sum.ofLongs(), "total_bytes_inspected"))
        .apply("MergePartialStatsRow", MapElements.via(new MergeLogAggrMap()));
  }

  public class MergeLogAggrMap extends SimpleFunction<Row, Row> {
    @Override
    public Row apply(Row input) {
      Row aggrRow =
          Row.withSchema(Util.bqAuditSchema)
              .addValues(
                  input.getRow("key").getString("source_file"),
                  Util.getTimeStamp(),
                  input.getRow("value").getInt64("total_bytes_inspected").longValue(),
                  Util.INSPECTED)
              .build();
      LOG.info("Audit Row {}", aggrRow.toString());
      return aggrRow;
    }
  }
}
