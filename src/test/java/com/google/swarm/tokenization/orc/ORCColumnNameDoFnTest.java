/*
 * Copyright 2023 Google LLC
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
package com.google.swarm.tokenization.orc;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class ORCColumnNameDoFnTest {

  @Rule public transient TestPipeline testPipeline = TestPipeline.create();

  @ClassRule public static TemporaryFolder tmpFolder = new TemporaryFolder();

  private static final String project_id = "mock-project-id";

  @Test
  public void testORCColumnNameDoFn() throws IOException {
    Integer numRecords = 2;
    List<String> fieldNames = new ArrayList<>();
    ORCTestUtil orcUtil = new ORCTestUtil(numRecords, tmpFolder);
    String testFilePath = orcUtil.generateORCFile();

    fieldNames.add("column_name1");
    fieldNames.add("column_name2");
    fieldNames.add("column_name3");

    PCollection<KV<String, List<String>>> results =
        testPipeline
            .apply(FileIO.match().filepattern(testFilePath))
            .apply(FileIO.readMatches().withCompression(Compression.AUTO))
            .apply(WithKeys.of("some_key"))
            .apply(ParDo.of(new ORCColumnNameDoFn(project_id)));

    PAssert.that(results).containsInAnyOrder(KV.of("some_key", fieldNames));

    testPipeline.run();
  }
}
