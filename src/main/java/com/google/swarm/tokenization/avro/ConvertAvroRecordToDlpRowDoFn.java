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

import com.google.privacy.dlp.v2.Table;
import com.google.privacy.dlp.v2.Value;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

public class ConvertAvroRecordToDlpRowDoFn extends DoFn<KV<String, GenericRecord>, KV<String, Table.Row>> {

    @ProcessElement
    public void processElement(ProcessContext c) {
        String key = c.element().getKey();
        GenericRecord record = c.element().getValue();
        Table.Row.Builder rowBuilder = Table.Row.newBuilder();
        AvroUtil.getFlattenedValues(record, (Object value) -> {
            if (value == null) {
                rowBuilder.addValues(Value.newBuilder().setStringValue("").build());
            } else {
                rowBuilder.addValues(Value.newBuilder().setStringValue(value.toString()).build());
            }
            c.output(KV.of(key, rowBuilder.build()));
        });
    }

}