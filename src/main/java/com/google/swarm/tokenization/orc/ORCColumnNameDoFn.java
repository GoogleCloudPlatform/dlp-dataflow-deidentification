package com.google.swarm.tokenization.orc;

import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.orc.Reader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

public class ORCColumnNameDoFn extends DoFn<KV<String, FileIO.ReadableFile>, KV<String, List<String>>> {

    public static final Logger LOG = LoggerFactory.getLogger(ORCColumnNameDoFn.class);

    public String projectId;

    public ORCColumnNameDoFn(String projectId) {
        this.projectId = projectId;
    }

    @ProcessElement
    public void processElement(ProcessContext c) throws IOException {
        FileIO.ReadableFile orcFile = c.element().getValue();
        String fileName = c.element().getKey();
        String filePath = orcFile.getMetadata().resourceId().toString();

        Reader reader = new ORCFileReader().createORCFileReader(filePath, projectId);
        List<String> fieldNames = reader.getSchema().getFieldNames();

        c.output(KV.of(fileName, fieldNames));
    }
}
