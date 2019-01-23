package com.google.swarm.tokenization.common;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.api.services.storage.Storage;
import com.google.privacy.dlp.v2.Value;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.junit.Test;
import com.google.privacy.dlp.v2.Table;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.nio.channels.ReadableByteChannel;
import java.util.*;

import com.google.privacy.dlp.v2.FieldId;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class UtilTest {

    @Test
    public void testParseBucketName() {
    String sampleString, output;

    sampleString = "gs://name/";
    output = Util.parseBucketName(sampleString);

    assertEquals("name", output);

    }

    @Test
    public void testConvertCsvRowToTableRow() {
        String csvRow = "this,is,a,sentence";
        Table.Row result = Util.convertCsvRowToTableRow(csvRow);

        assertEquals(result.getValuesCount(), 4);
        assertEquals(result.getValues(0).getStringValue(),"this");
        assertEquals(result.getValues(1).getStringValue(),"is");
        assertEquals(result.getValues(2).getStringValue(),"a");
        assertEquals(result.getValues(3).getStringValue(),"sentence");

    }

    @Test
    public void countRecords() {
        String test_string = "Hello \n World \n How are you?";
        Reader inputString = new StringReader(test_string);
        BufferedReader reader = new BufferedReader(inputString);
        Integer result = Util.countRecords(reader);


        assertEquals(result.intValue(),3);
    }

    @Test
    public void getHeaders() throws IOException {
        String header_input = "Column1,Column2,Column3";
        Reader inputString = new StringReader(header_input);
        BufferedReader reader = new BufferedReader(inputString);
        List<FieldId> result = Util.getHeaders(reader);

        assertEquals(result.get(0).getName(), "Column1");
        assertEquals(result.get(1).getName(), "Column2");
        assertEquals(result.get(2).getName(), "Column3");
    }

    @Test
    public void testCreateDLPTable() {
        List <FieldId> output = new ArrayList<FieldId>();
        output.add(FieldId.newBuilder().setName("First").build());
        output.add(FieldId.newBuilder().setName("Second").build());

        List <String> sArray = new ArrayList<String>();
        sArray.add("t1,t2");
        sArray.add("t3,t4");

        ArrayList<ArrayList<Value>> rows = new ArrayList<ArrayList<Value>>();
        for (String s: sArray){
            ArrayList<Value> row1 = new ArrayList<Value>();
            String[] values = s.split(",");
            for (String value :values) {
                row1.add(Value.newBuilder().setStringValue(value).build());
            }
            rows.add(row1);
        }

        List<Table.Row> tableRows = new ArrayList<Table.Row>();
        for (ArrayList<Value> row1: rows) {
            Table.Row.Builder test = Table.Row.newBuilder();
            tableRows.add(test.addAllValues(row1).build());
        }

        Table result = Util.createDLPTable(output, sArray);
        assertEquals(result.getHeadersList(), output);
        assertEquals(result.getRowsList(), tableRows);


    }

    @Test
    public void testFindEncryptionType() {
        assertEquals(Util.findEncryptionType(null, "Test", "Test", "Test"),true);
        assertEquals(Util.findEncryptionType(null, null, null, null),false);
    }
/*
    @Test
        public void testGetReader() throws IOException {
        FileIO.ReadableFile f = mock(FileIO.ReadableFile.class);
        ValueProvider<String> testValueProvider = mock(ValueProvider.class);
        ReadableByteChannel b = mock(ReadableByteChannel.class);


        BufferedReader test1 = Util.getReader(false, "object_name",
                        "bucket_name", f, "key_name", testValueProvider);


        assertNotNull(test1);
        }
*/

    @Test
    public void testGetSchema() {
        List<String> schema = new ArrayList<String>();
        schema.add("Column1");
        schema.add("Column2");

        TableSchema result = Util.getSchema(schema);

        assertEquals(result.getFields().get(0).getName(), "Column1" );
        assertEquals(result.getFields().get(1).getName(), "Column2" );
        assertEquals(result.getFields().get(0).getType(), "STRING");
        assertEquals(result.getFields().get(1).getType(), "STRING");

    }

    @Test
    public void testToJsonString() throws IOException {
        Map<String, String> input = new HashMap<String, String>();
        input.put("key1","value1");
        input.put("key2","value2");

        String output_string = Util.toJsonString(input);

        ObjectMapper mapper = new ObjectMapper();
        Map<String,Object> map = mapper.readValue(output_string, Map.class);

        assertEquals(map.get("key1"),"value1");
        assertEquals(map.get("key2"),"value2");

    }

    @Test
    public void testExtractTableHeader() {
        List <FieldId> output = new ArrayList<FieldId>();
        output.add(FieldId.newBuilder().setName("First").build());
        output.add(FieldId.newBuilder().setName("Second").build());

        Table inputTable = Table.newBuilder().addAllHeaders(output).build();
        String result = Util.extractTableHeader(inputTable);

        assertEquals(result,"First,Second\n");




    }
}