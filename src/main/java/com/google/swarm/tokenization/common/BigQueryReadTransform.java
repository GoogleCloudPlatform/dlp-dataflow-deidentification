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

import autovalue.shaded.org.checkerframework.checker.nullness.qual.Nullable;
import com.google.api.services.bigquery.model.TableRow;
import com.google.auto.value.AutoValue;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.TypedRead.Method;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@AutoValue
@SuppressWarnings({"deprecation", "serial"})
public abstract class BigQueryReadTransform
    extends PTransform<PBegin, PCollection<KV<String, TableRow>>> {
  public static final Logger LOG = LoggerFactory.getLogger(BigQueryReadTransform.class);

  public abstract String tableRef();

  public abstract Method readMethod();

  public abstract Integer keyRange();

  public abstract String query();

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setTableRef(String tableRef);

    public abstract Builder setReadMethod(Method readMethod);

    public abstract Builder setKeyRange(Integer keyRange);

    @Nullable
    public abstract Builder setQuery(String query);

    public abstract BigQueryReadTransform build();
  }

  public static Builder newBuilder() {
    return new AutoValue_BigQueryReadTransform.Builder();
  }

  @Override
  public PCollection<KV<String, TableRow>> expand(PBegin input) {

    switch (readMethod()) {
      case DEFAULT:
        if(query()!=null) {
          return input
                  .apply(
                          "ReadFromBigQuery",
                          BigQueryIO.readTableRows()
                                  .fromQuery(query())
                                  .usingStandardSql()
                                  .withMethod(Method.DEFAULT))
                  .apply("AddTableNameAsKey", WithKeys.of(tableRef()));
        }
        else {
          return input
                  .apply(
                          "ReadFromBigQuery",
                          BigQueryIO.readTableRows()
                                  .from(tableRef())
                                  .withMethod(Method.DEFAULT))
                  .apply("AddTableNameAsKey", WithKeys.of(tableRef()));
        }

      case EXPORT:
        throw new IllegalArgumentException("Export method not supported");

      case DIRECT_READ:
        throw new IllegalArgumentException("Direct read not supported");

      default:
        throw new IllegalArgumentException("Not a valid method");
    }
  }
}
