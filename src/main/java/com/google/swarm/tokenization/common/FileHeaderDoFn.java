package com.google.swarm.tokenization.common;

import java.io.BufferedReader;
import java.io.IOException;
import org.apache.beam.sdk.io.FileIO.ReadableFile;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileHeaderDoFn extends DoFn<KV<String, Iterable<ReadableFile>>, String> {
  public static final Logger LOG = LoggerFactory.getLogger(FileHeaderDoFn.class);

  @ProcessElement
  public void processElement(ProcessContext c) {
    ReadableFile file = c.element().getValue().iterator().next();
    try (BufferedReader br = Util.getReader(file)) {
      CSVRecord csvHeader = CSVFormat.newFormat('|').parse(br).getRecords().get(0);
      csvHeader.forEach(
          headerValue -> {
            c.output(headerValue);
          });
    } catch (IOException e) {
      LOG.error("Failed to get csv header values}", e.getMessage());
      throw new RuntimeException(e);
    }
  }
}
