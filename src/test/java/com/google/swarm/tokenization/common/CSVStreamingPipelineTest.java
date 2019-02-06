package com.google.swarm.tokenization.common;

import com.google.cloud.dlp.v2.DlpServiceClient;
import com.google.privacy.dlp.v2.*;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Keys;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;

import org.apache.beam.sdk.testing.TestPipeline;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.any;
import static org.junit.Assert.*;


public class CSVStreamingPipelineTest {

    @Rule
    public final transient TestPipeline pipeline = TestPipeline.create();

    @Rule
    public final ExpectedException exception = ExpectedException.none();


    @Test
    public void testCSVContentProcessorDoFn(){
        List<String> sampleStringList = new ArrayList<String>();
        sampleStringList.add("A,a");
        sampleStringList.add("B,b");
        sampleStringList.add("C,c");
        sampleStringList.add("D,c");

        PCollection<KV<String, List<String>>> input  = pipeline.apply(Create.of(
                KV.of("Test1", sampleStringList.subList(0,1)),
                KV.of("Test2", sampleStringList.subList(1,2)),
                KV.of("Test3", sampleStringList.subList(2,3))
        ));

        PCollection<KV<String, Table>> outputTables= input.apply("ContentHandler",
                ParDo.of(new CSVContentProcessorDoFn(ValueProvider.StaticValueProvider.of(100))));

        PCollection<String> outputKeys = outputTables.apply(Keys.create());
        assertNotNull(outputKeys);
        //PAssert.that(outputKeys).empty();
        //PAssert.that(outputKeys).containsInAnyOrder("Test1_1");
        pipeline.run();

    }

    @Test
    public void testDLPTokenizationDoFn() throws SQLException {
        String headerString = "Header1,Header2";
        List<FieldId> headers = Arrays.stream(headerString.split(","))
                .map(header -> FieldId.newBuilder().setName(header).build()).collect(Collectors.toList());

        List<String> lines = new ArrayList<>();
        lines.add("test1,test2");
        lines.add("foo,bar");
        Table batchData = Util.createDLPTable(headers, lines);

        DLPTokenizationDoFn mockDLP = mock(DLPTokenizationDoFn.class, Mockito.RETURNS_DEEP_STUBS);
        DlpServiceClient mockServiceClient = mock(DlpServiceClient.class, Mockito.RETURNS_DEEP_STUBS);
        //DeidentifyContentResponse mockResponse = mock(DeidentifyContentResponse.class);

        when(mockDLP.createDLPServiceClient()).thenReturn(mockServiceClient);
        when(mockServiceClient.deidentifyContent(any(DeidentifyContentRequest.class)))
                .thenThrow(new InterruptedException("DLP Client has deidentified"));
        //when(mockServiceClient.deidentifyContent(any(DeidentifyContentRequest.class)))
        //        .thenReturn(mockResponse);

        PCollection<KV<String, Table>> input =
                pipeline.apply(Create.of(KV.of("File1_1", batchData)));

        exception.expect(InterruptedException.class);
        PCollection<Row> tokenizedData =
                input.apply("Tokenize", ParDo.of(mockDLP));

        pipeline.run();




    }


}
