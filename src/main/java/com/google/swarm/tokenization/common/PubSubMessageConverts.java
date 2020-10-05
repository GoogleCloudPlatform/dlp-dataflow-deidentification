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
package com.google.swarm.tokenization.common;

import com.google.api.services.bigquery.model.TableRow;
import com.google.common.collect.ImmutableMap;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;

@SuppressWarnings("serial")
public class PubSubMessageConverts extends SimpleFunction<KV<String, TableRow>, PubsubMessage> {
  @Override
  public PubsubMessage apply(KV<String, TableRow> input) {

    String tableRef = input.getKey();
    String json = input.getValue().toString();
    PubsubMessage message =
        new PubsubMessage(
            json.getBytes(),
            ImmutableMap.<String, String>builder().put("table_name", tableRef).build());
    return message;
  }
}
