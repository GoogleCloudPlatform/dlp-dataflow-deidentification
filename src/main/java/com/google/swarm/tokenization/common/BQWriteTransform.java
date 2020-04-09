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

import com.google.auto.value.AutoValue;
import javax.annotation.Nullable;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@AutoValue
public abstract class BQWriteTransform extends PTransform<PCollection<Row>, WriteResult> {
  private static final Logger LOG = LoggerFactory.getLogger(BQWriteTransform.class);
  private static final Integer NUM_OF_SHARDS = 100;

  @Nullable
  public abstract Integer batchFrequency();

  public abstract BigQueryIO.Write.Method method();

  public abstract String tableSpec();

  public static Builder newBuilder() {
    return new AutoValue_BQWriteTransform.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder setBatchFrequency(Integer batchFrequency);

    public abstract Builder setTableSpec(String tableSpec);

    public abstract Builder setMethod(BigQueryIO.Write.Method method);

    public abstract BQWriteTransform build();
  }

  @Override
  public WriteResult expand(PCollection<Row> input) {
    switch (method()) {
      case FILE_LOADS:
        return input.apply(
            BigQueryIO.<Row>write()
                .to(tableSpec())
                .useBeamSchema()
                .withMethod(BigQueryIO.Write.Method.FILE_LOADS)
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER)
                .withTriggeringFrequency(Duration.standardMinutes(batchFrequency()))
                .withNumFileShards(NUM_OF_SHARDS));
      case STREAMING_INSERTS:
        return input.apply(
            BigQueryIO.<Row>write()
                .to(tableSpec())
                .withMethod(BigQueryIO.Write.Method.STREAMING_INSERTS)
                .useBeamSchema()
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER));
      default:
        return input.apply(
            BigQueryIO.<Row>write()
                .to(tableSpec())
                .withMethod(method())
                .useBeamSchema()
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER));
    }
  }
}
