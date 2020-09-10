/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.swarm.tokenization.beam;

import com.google.privacy.dlp.v2.Table;
import com.google.privacy.dlp.v2.Value;

import java.io.IOException;
import java.util.Objects;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

/**
 * Maps {@link KV}s of {@link String}s into KV<{@link String}, {@link Table.Row}> for further
 * processing in the DLP transforms.
 *
 * <p>If a column delimiter of values isn't provided, input is assumed to be unstructured and the input KV
 * value is saved in a single column of output {@link Table.Row}.
 */
public class ConvertCSVRecordToDLPRow extends DoFn<KV<String, String>, KV<String, Table.Row>> {

    private final Character columnDelimiter;

    public ConvertCSVRecordToDLPRow() {
        this(null);
    }

    public ConvertCSVRecordToDLPRow(Character columnDelimiter) {
        this.columnDelimiter = columnDelimiter;
    }

    @ProcessElement
    public void processElement(ProcessContext context) throws IOException {
        Table.Row.Builder rowBuilder = Table.Row.newBuilder();
        String input = Objects.requireNonNull(context.element().getValue());
        if (columnDelimiter != null) {
            CSVFormat csvFormat = CSVFormat.DEFAULT.withDelimiter(columnDelimiter);
            CSVParser records = CSVParser.parse(input, csvFormat);
            CSVRecord record = records.getRecords().get(0);
            record.iterator().forEachRemaining(
                value -> rowBuilder.addValues(
                    Value.newBuilder().setStringValue(value).build()
                )
            );
        } else {
            rowBuilder.addValues(
                Value.newBuilder().setStringValue(input).build()
            );
        }
        context.output(KV.of(context.element().getKey(), rowBuilder.build()));
    }
}
