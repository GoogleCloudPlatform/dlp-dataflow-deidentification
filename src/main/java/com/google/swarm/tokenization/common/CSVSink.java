/*
 * Copyright 2018 Google LLC
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
package com.google.swarm.tokenization.common;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.beam.sdk.io.FileIO.Sink;
import org.apache.beam.sdk.values.KV;

@SuppressWarnings("serial")
public class CSVSink implements Sink<KV<String, Iterable<Row>>> {

  private PrintWriter writer;

  @Override
  public void open(WritableByteChannel channel) throws IOException {
    writer = new PrintWriter(Channels.newOutputStream(channel));
  }

  @Override
  public void write(KV<String, Iterable<Row>> element) throws IOException {

    Iterator<Row> valueIterator = element.getValue().iterator();
    StringBuilder csvRows = new StringBuilder();
    String csvHeader = getHeader(element.getValue().iterator().next());
    if (csvHeader != null) csvRows.append(csvHeader + "\n");
    while (valueIterator.hasNext()) {
      Row row = valueIterator.next();
      List<String> value = Arrays.asList(row.getValue());
      String csvRow = value.stream().collect(Collectors.joining(","));
      csvRows.append(csvRow + "\n");
    }

    writer.println(csvRows);
  }

  @Override
  public void flush() throws IOException {
    writer.flush();
  }

  private String getHeader(Row row) {
    String csvHeader = null;
    if (row != null) {
      List<String> headers = Arrays.asList(row.getHeader());
      csvHeader = headers.stream().collect(Collectors.joining(","));
    }
    return csvHeader;
  }
}
