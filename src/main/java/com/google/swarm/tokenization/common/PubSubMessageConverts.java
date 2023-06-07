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
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("serial")
public class PubSubMessageConverts extends DoFn<KV<String, TableRow>, PubsubMessage> {
  private static final Logger LOG = LoggerFactory.getLogger(PubSubMessageConverts.class);

  @ProcessElement
  public void processContext(ProcessContext c) {
    String tableRef = c.element().getKey();
    TableRow row = c.element().getValue();
    String json = Util.gson.toJson(row.getF());
    PubsubMessage message =
        new PubsubMessage(
            json.getBytes(),
            ImmutableMap.<String, String>builder().put("table_name", tableRef).build());
    c.output(message);
  }
}
