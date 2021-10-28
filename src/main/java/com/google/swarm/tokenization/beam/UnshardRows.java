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
package com.google.swarm.tokenization.beam;

import com.google.privacy.dlp.v2.Table.Row;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.ShardedKey;

public class UnshardRows
    extends DoFn<KV<ShardedKey<String>, Iterable<Row>>, KV<String, Iterable<Row>>> {

  @ProcessElement
  public void process(
      @Element KV<ShardedKey<String>, Iterable<Row>> e,
      OutputReceiver<KV<String, Iterable<Row>>> outputReceiver) {
    outputReceiver.output(KV.of(e.getKey().getKey(), e.getValue()));
  }
}
