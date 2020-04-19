package com.google.swarm.tokenization.common;

import org.apache.beam.sdk.schemas.transforms.Group;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AudioInspectDataTransform extends PTransform<PCollection<Row>, PCollection<Row>> {
  public static final Logger LOG = LoggerFactory.getLogger(AudioInspectDataTransform.class);

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

  public class MergeLogAggrMap extends SimpleFunction<KV<Row, Row>, Row> {
    @Override
    public Row apply(KV<Row, Row> input) {
      Row aggrRow =
          Row.withSchema(Util.bqAuditSchema)
              .addValues(
                  input.getKey().getString("source_file"),
                  Util.getTimeStamp(),
                  input.getValue().getInt64("total_bytes_inspected").longValue(),
                  Util.INSPECTED)
              .build();
      LOG.info("Audit Row {}", aggrRow.toString());
      return aggrRow;
    }
  }
}
