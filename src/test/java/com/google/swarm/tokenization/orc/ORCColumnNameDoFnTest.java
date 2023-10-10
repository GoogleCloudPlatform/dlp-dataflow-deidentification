package com.google.swarm.tokenization.orc;

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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ORCColumnNameDoFnTest {

    @Rule
    public transient TestPipeline testPipeline = TestPipeline.create();

    @ClassRule
    public static TemporaryFolder tmpFolder = new TemporaryFolder();

    protected static final String PROJECT_ID = "test-project";

    @Test
    public void testORCColumnNameDoFn() throws IOException {
        Integer numRecords = 2;
        List<String> fieldNames = new ArrayList<>();
        ORCUtil orcUtil = new ORCUtil(numRecords, tmpFolder);
        String testFilePath = orcUtil.generateORCFile();

        fieldNames.add("column_name1");
        fieldNames.add("column_name2");
        fieldNames.add("column_name3");

        PCollection<KV<String, List<String>>> results = testPipeline
                        .apply(FileIO.match().filepattern(testFilePath))
                        .apply(FileIO.readMatches().withCompression(Compression.AUTO))
                        .apply(WithKeys.of("some_key"))
                        .apply(ParDo.of(new ORCColumnNameDoFn(PROJECT_ID)));

        PAssert.that(results).containsInAnyOrder(KV.of("some_key", fieldNames));

        testPipeline.run();
    }
}
