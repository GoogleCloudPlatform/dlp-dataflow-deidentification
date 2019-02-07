package com.google.swarm.tokenization.common;

import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Keys;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;

import org.apache.beam.sdk.testing.TestPipeline;
import org.junit.rules.ExpectedException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.times;
import static org.junit.Assert.*;


public class CSVStreamingPipelineTest {
    /*
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
        //assertNotNull(outputKeys);
        //PAssert.that(outputKeys).empty();
        PAssert.that(outputKeys).containsInAnyOrder("Test1_1");
        pipeline.run();

    }*/

    @Test
    public void testCSVStreamingInitialRestriction(){
        CSVContentProcessorDoFn csv =
                new CSVContentProcessorDoFn(ValueProvider.StaticValueProvider.of(2));
        String[] lines1 = {"line1", "line2", "line3", "line4"};
        String[] lines2 = {"line1", "line2", "line3", "line4", "line5", "line6"};

        KV<String, List<String>> input1 = KV.of("FileName", Arrays.asList(lines1));
        KV<String, List<String>> input2 = KV.of("FileName", Arrays.asList(lines2));

        OffsetRange rangeResult1 = csv.getInitialRestriction(input1);
        assertEquals(rangeResult1.getFrom(),1);
        assertEquals(rangeResult1.getTo(),3);


        OffsetRange rangeResult2 = csv.getInitialRestriction(input2);
        assertEquals(rangeResult2.getFrom(),1);
        assertEquals(rangeResult2.getTo(),4);


    }

    @Test
    public void testSplitRestriction(){
        CSVContentProcessorDoFn csv =
                new CSVContentProcessorDoFn(ValueProvider.StaticValueProvider.of(2));
        OffsetRange off = new OffsetRange(2,5);
        DoFn.OutputReceiver out = mock(DoFn.OutputReceiver.class);


        csv.splitRestriction(off, out);
        verify(out, times(3)).output(any(OffsetRange.class));

    }

    @Test
    public void testNewTracker(){
        CSVContentProcessorDoFn csv =
                new CSVContentProcessorDoFn(ValueProvider.StaticValueProvider.of(2));
        OffsetRange off = new OffsetRange(2,5);
        org.apache.beam.sdk.transforms.splittabledofn.OffsetRangeTracker offTrack = csv.newTracker(off);
        assertEquals(offTrack.currentRestriction(),off);


    }

    @Test
    public void testCreateLineBatches(){
        CSVContentProcessorDoFn csv =
                new CSVContentProcessorDoFn(ValueProvider.StaticValueProvider.of(2));
        long i = 2;
        KV<Integer,Integer> result1 = csv.createStartEnd(6,2);
        KV<Integer,Integer> result2 = csv.createStartEnd(6,1);
        assertEquals(result1, KV.of(3,4));
        assertEquals(result2, KV.of(1,2));


    }






}
