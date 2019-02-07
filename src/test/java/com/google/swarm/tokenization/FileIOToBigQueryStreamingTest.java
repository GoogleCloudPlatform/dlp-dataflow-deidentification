package com.google.swarm.tokenization;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v20.com.google.common.base.Charsets;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.privacy.dlp.v2.Table;
import com.google.swarm.tokenization.FileIOToBigQueryStreaming.CSVReader;

@RunWith(JUnit4.class)

public class FileIOToBigQueryStreamingTest {

	public static final Logger LOG = LoggerFactory.getLogger(FileIOToBigQueryStreamingTest.class);

	@Rule
	public final transient TestPipeline p = TestPipeline.create();

	@ClassRule
	public static TemporaryFolder tempFolder = new TemporaryFolder();

	private static final String FILE_BASE_NAME = "tokenization_data.csv";

	private static StringBuilder FILE_CONTENT = new StringBuilder();

	private static String tempFolderUncompressedPath;

	@BeforeClass
	public static void setupClass() throws IOException {
	
		tempFolderUncompressedPath = tempFolder.newFile(FILE_BASE_NAME).getAbsolutePath();
		FILE_CONTENT.append(
				"CardTypeCode,CardTypeFullName,IssuingBank,CardNumber,CardHoldersName,CVVCVV2,IssueDate,ExpiryDate,BillingDate,CardPIN,CreditLimit,\n");
		FILE_CONTENT
				.append("MC,Master Card,Wells Fargo,E5ssxfuqnGfF36Kk,Jeremy O Wilson,NK3,12/2007,12/2008,3,vmFF,19800");

		Files.write(new File(tempFolderUncompressedPath).toPath(), FILE_CONTENT.toString().getBytes(Charsets.UTF_8));

	}

	@After
	public void cleanupTestEnvironment() {
		tempFolder.delete();
	}

	@Test
	public void testFileIOToBigQueryStreamingE2E() throws IOException {
		ValueProvider<Integer> batchSize = p.newProvider(10);

		PCollection<KV<String, Table>> dlpTable = p
				.apply("Match", FileIO.match().filepattern(tempFolderUncompressedPath))
				.apply("Read File", FileIO.readMatches().withCompression(Compression.UNCOMPRESSED))
				.apply("Create DLP Table", ParDo.of(new CSVReader(batchSize)));

		PAssert.that(dlpTable).satisfies(collection -> {

			KV<String, Table> tableData = collection.iterator().next();
			assertThat(tableData.getKey(), is(equalTo("tokenization_data_1")));
			assertThat(tableData.getValue().getHeadersCount(), is(equalTo(11)));
			assertThat(tableData.getValue().getRowsCount(), is(equalTo(1)));
			return null;
		});
		p.run();
	}

}
