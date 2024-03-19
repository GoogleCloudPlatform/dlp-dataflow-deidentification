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
import com.google.swarm.tokenization.options.CommonPipelineOptions;
import java.util.Random;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.ShardedKeyCoder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.ShardedKey;

public class ShardRows
    extends PTransform<PCollection<KV<String, Row>>, PCollection<KV<ShardedKey<String>, Row>>> {

  private static final Random RANDOM_GENERATOR = new Random();
  private static final int UNDEFINED_NUMBER_OF_SHARDS = Integer.MIN_VALUE;
  private int numberOfShards;

  public ShardRows() {
    numberOfShards = UNDEFINED_NUMBER_OF_SHARDS;
  }

  public ShardRows(int numberOfShards) {
    this.numberOfShards = numberOfShards;
  }

  @Override
  public PCollection<KV<ShardedKey<String>, Row>> expand(PCollection<KV<String, Row>> input) {
    Coder<String> stringCoder = null;
    Coder<Row> rowCoder = null;
    try {
      stringCoder = input.getPipeline().getCoderRegistry().getCoder(String.class);
      rowCoder = input.getPipeline().getCoderRegistry().getCoder(Row.class);
    } catch (CannotProvideCoderException e) {
      throw new RuntimeException(e);
    }

    if (numberOfShards == UNDEFINED_NUMBER_OF_SHARDS) {
      CommonPipelineOptions options =
          input.getPipeline().getOptions().as(CommonPipelineOptions.class);
      numberOfShards = options.getNumShardsPerDLPRequestBatching();
    }

    return input
        .apply(
            "Shard",
            ParDo.of(
                new DoFn<KV<String, Row>, KV<ShardedKey<String>, Row>>() {
                  @ProcessElement
                  public void process(
                      @Element KV<String, Row> e,
                      OutputReceiver<KV<ShardedKey<String>, Row>> outputReceiver) {
                    outputReceiver.output(
                        KV.of(
                            ShardedKey.of(e.getKey(), RANDOM_GENERATOR.nextInt(numberOfShards)),
                            e.getValue()));
                  }
                }))
        .setCoder(KvCoder.of(ShardedKeyCoder.of(stringCoder), rowCoder));
  }
}
