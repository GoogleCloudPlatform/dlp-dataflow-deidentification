package com.google.swarm.tokenization.orc;

import com.google.swarm.tokenization.avro.AvroColumnNamesDoFn;
import com.google.swarm.tokenization.avro.AvroUtil;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ORCColumnNameDoFn extends DoFn<KV<String, FileIO.ReadableFile>, KV<String, List<String>>> {

    public static final Logger LOG = LoggerFactory.getLogger(ORCColumnNameDoFn.class);

    @ProcessElement
    public void processElement(ProcessContext c) {
        FileIO.ReadableFile avroFile = c.element().getValue();
        String fileName = c.element().getKey();
        c.output(KV.of(fileName, new ArrayList<String>()));
    }


}
