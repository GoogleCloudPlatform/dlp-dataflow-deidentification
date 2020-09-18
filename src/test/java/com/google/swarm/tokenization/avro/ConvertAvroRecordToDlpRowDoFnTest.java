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
package com.google.swarm.tokenization.avro;

import static com.google.swarm.tokenization.avro.AvroReaderSplitDoFnTest.generateDLPRows;
import static com.google.swarm.tokenization.avro.AvroReaderSplitDoFnTest.generateGenericRecords;
import static com.google.swarm.tokenization.avro.AvroReaderSplitDoFnTest.schema;

import com.google.privacy.dlp.v2.Table;
import java.util.List;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;

public class ConvertAvroRecordToDlpRowDoFnTest {

  @Rule public transient TestPipeline pipeline = TestPipeline.create();

  @Test
  public void testConvertAvroRecordToDlpRowDoFn() {
    int numRecords = 10;
    List<GenericRecord> records = generateGenericRecords(numRecords);
    List<Table.Row> rows = generateDLPRows(numRecords);

    PCollection<Table.Row> results =
        pipeline
            .apply(Create.of(records).withCoder(AvroCoder.of(schema)))
            .apply(WithKeys.of("some_key"))
            .setCoder(KvCoder.of(StringUtf8Coder.of(), AvroCoder.of(schema)))
            .apply(ParDo.of(new ConvertAvroRecordToDlpRowDoFn()))
            .apply(Values.create());

    PAssert.that(results).containsInAnyOrder(rows);

    pipeline.run();
  }
}
