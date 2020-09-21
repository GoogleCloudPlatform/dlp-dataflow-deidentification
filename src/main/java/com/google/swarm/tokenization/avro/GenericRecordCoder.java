/*
 * Copyright 2020 Google LLC
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
package com.google.swarm.tokenization.avro;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Base64;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;

public class GenericRecordCoder extends AtomicCoder<GenericRecord> {

  // Cache that holds AvroCoders for the encountered Avro records
  private static final ConcurrentHashMap<String, AvroCoder<GenericRecord>> codersCache =
      new ConcurrentHashMap<>();

  public static GenericRecordCoder of() {
    return new GenericRecordCoder();
  }

  @Override
  public void encode(GenericRecord record, OutputStream outputStream) throws IOException {
    // Add base64-encoded schema to the output stream
    String schema = record.getSchema().toString();
    String schema64 = Base64.getEncoder().encodeToString(schema.getBytes());
    StringUtf8Coder.of().encode(schema64, outputStream);

    // Get AvroCoder associated with the schema
    AvroCoder<GenericRecord> coder =
        codersCache.computeIfAbsent(schema64, key -> AvroCoder.of(record.getSchema()));

    // Add encoded record to the output stream
    coder.encode(record, outputStream);
  }

  @Override
  public GenericRecord decode(InputStream inputStream) throws IOException {
    // Fetch the base64-encoded schema
    String schema64 = StringUtf8Coder.of().decode(inputStream);

    // Get AvroCoder associated with the schema
    AvroCoder<GenericRecord> coder =
        codersCache.computeIfAbsent(
            schema64,
            key ->
                AvroCoder.of(
                    new Schema.Parser()
                        .parse(Arrays.toString(Base64.getDecoder().decode(schema64)))));

    // Decode the Avro record
    return coder.decode(inputStream);
  }
}
