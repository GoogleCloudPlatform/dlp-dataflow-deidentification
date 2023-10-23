/*
 * Copyright 2023 Google LLC
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

import java.util.List;
import org.apache.beam.sdk.transforms.Combine;

public class ResolveDuplicatesCombineFn
    extends Combine.CombineFn<List<String>, List<String>, List<String>> {

  @Override
  public List<String> createAccumulator() {
    return null;
  }

  @Override
  public List<String> addInput(List<String> accum, List<String> input) {
    return input;
  }

  @Override
  public List<String> extractOutput(List<String> accum) {
    return accum;
  }

  @Override
  public List<String> mergeAccumulators(Iterable<List<String>> values) {
    return values.iterator().next();
  }
}
