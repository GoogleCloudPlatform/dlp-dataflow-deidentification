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
import com.google.privacy.dlp.v2.Value;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.junit.Rule;
import org.junit.Test;

public class ConvertCSVRecordToDLPRowTest {

  @Rule public transient TestPipeline pipeline = TestPipeline.create();

  @Test
  public void testConvertCSVRecordToDLPRow() {

    final PCollectionView<List<String>> header =
        pipeline.apply("create header", Create.of("h1", "h2", "h3")).apply(View.asList());
    // Define pipeline
    Create.Values<String> input = Create.of("a1,b1,c1");
    PCollection<Table.Row> results =
        pipeline
            .apply(input)
            .apply(WithKeys.of("some_key"))
            .apply(ParDo.of(new ConvertCSVRecordToDLPRow(',', header)).withSideInputs(header))
            .apply(Values.create());

    // Create expected results
    List<Table.Row> rows = new ArrayList<>();
    Table.Row row1 =
        Table.Row.newBuilder()
            .addValues(Value.newBuilder().setStringValue("a1").build())
            .addValues(Value.newBuilder().setStringValue("b1").build())
            .addValues(Value.newBuilder().setStringValue("c1").build())
            .build();
    rows.add(row1);

    // Check the pipeline's results
    PAssert.that(results).containsInAnyOrder(rows);

    pipeline.run();
  }
}
