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

import java.util.*;

import com.google.common.collect.HashMultiset;
import com.google.common.collect.Iterators;
import com.google.common.collect.Multiset;
import com.google.privacy.dlp.v2.Table;
import com.google.privacy.dlp.v2.Value;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.extensions.protobuf.ProtoCoder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import static org.junit.Assert.*;

public class BatchRequestForDLPTest {

    @Rule
    public final transient TestPipeline pipeline = TestPipeline.create();

    private static final Instant baseTime = new Instant(0);

    /**
     * Utility function that creates and returns a sample DLP row
     */
    private static Table.Row createRow() {
        Table.Row.Builder rowBuilder = Table.Row.newBuilder();
        Value arbitraryValue = Value.newBuilder().setIntegerValue(999).build();
        return rowBuilder.addValues(arbitraryValue).build();
    }

    @Test
    public void testBatching() {
        // Initialize the stream
        Coder<String> utf8Coder = StringUtf8Coder.of();
        Coder<Table.Row> rowCoder = ProtoCoder.of(Table.Row.class);
        KvCoder<String, Table.Row> keyValueCoder = KvCoder.of(utf8Coder, rowCoder);
        TestStream.Builder<KV<String, Table.Row>> streamBuilder = TestStream.create(keyValueCoder);

        // Stream some sample data
        String[] keys = new String[]{ "a", "b", "c", "b", "a", "a", "d", "a", "b", "a", "c", "b", "a" };
        for (String key : keys) {
            Table.Row row = createRow();
            streamBuilder = streamBuilder.addElements(
                TimestampedValue.of(KV.of(key, row), baseTime.plus(Duration.standardSeconds(1))));
        }

        // Finalize the stream
        TestStream<KV<String, Table.Row>> rows = streamBuilder.advanceWatermarkToInfinity();

        // Set an arbitrary window duration
        Duration windowDuration = Duration.standardMinutes(5);

        // Allow 3 items per batch
        int batchSize = createRow().getSerializedSize() * 3;

        // Define the pipeline
        PCollection<KV<String, Iterable<Table.Row>>> results =
            pipeline
                .apply(rows)
                .apply(Window.into(FixedWindows.of(windowDuration)))
                .apply(ParDo.of(new BatchRequestForDLP(batchSize)));

        // Inspect the pipeline's results
        PAssert
            .that(results)
            .inWindow(new IntervalWindow(baseTime, baseTime.plus(windowDuration)))
            .satisfies(batches -> {
                // Collect the number of rows in each bag for each key
                Map<String, Multiset<Integer>> actual = new HashMap<>();
                batches.forEach(batch -> {
                    int numRows = Iterators.size(batch.getValue().iterator());
                    Multiset<Integer> set = actual.getOrDefault(batch.getKey(), HashMultiset.create());
                    set.add(numRows);
                    actual.put(batch.getKey(), set);
                });
                // Compare the actual numbers with the expected ones
                Map<String, HashMultiset<Integer>> expected = Map.of(
                    "a", HashMultiset.create(Arrays.asList(3, 3)),
                    "b", HashMultiset.create(Arrays.asList(3, 1)),
                    "c", HashMultiset.create(Arrays.asList(2)),
                    "d", HashMultiset.create(Arrays.asList(1))
                );
                assertEquals(expected, actual);
                return null;
            });

        pipeline.run().waitUntilFinish();
    }

}
