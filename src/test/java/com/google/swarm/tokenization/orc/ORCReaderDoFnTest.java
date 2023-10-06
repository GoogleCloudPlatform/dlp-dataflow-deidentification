package com.google.swarm.tokenization.orc;

import com.google.privacy.dlp.v2.Table;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.testing.PAssert;

import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.values.PCollection;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.List;

public class ORCReaderDoFnTest {

    @Rule
    public transient TestPipeline testPipeline = TestPipeline.create();

    @ClassRule
    public static TemporaryFolder tmpFolder = new TemporaryFolder();

    protected static final String PROJECT_ID = "test-project";

    @Test
    public void testORCReaderDoFn() throws IOException {
        Integer numRecords = 2;
        ORCUtil orcUtil = new ORCUtil(numRecords, tmpFolder);
        String testFilePath = orcUtil.generateORCFile();
        List<Table.Row> tableRows = orcUtil.generateTableRows();

        PCollection<Table.Row> results = testPipeline
                .apply(FileIO.match().filepattern(testFilePath))
                .apply(FileIO.readMatches().withCompression(Compression.AUTO))
                .apply(WithKeys.of("some_key"))
                .apply(ParDo.of(new ORCReaderDoFn(PROJECT_ID)))
                .apply(Values.create());

        PAssert.that(results).containsInAnyOrder(tableRows);

        testPipeline.run();
    }
}
