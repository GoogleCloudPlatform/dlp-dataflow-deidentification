/*
 * Copyright 2024 Google LLC
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
package com.google.swarm.tokenization.classification;

import com.google.privacy.dlp.v2.InspectContentResponse;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProcessInspectFindingsDoFn
    extends DoFn<KV<String, InspectContentResponse>, KV<String, Long>> {

  public static final TupleTag<KV<String, Long>> inspectSuccessTag =
      new TupleTag<KV<String, Long>>() {};
  public static final TupleTag<KV<String, Long>> inspectFailureTag =
      new TupleTag<KV<String, Long>>() {};

  public static final Logger LOG = LoggerFactory.getLogger(ProcessInspectFindingsDoFn.class);

  public static TupleTag<KV<String, Long>> getInspectSuccessTag() {
    return inspectSuccessTag;
  }

  public static TupleTag<KV<String, Long>> getInspectFailureTag() {
    return inspectFailureTag;
  }

  @ProcessElement
  public void processElement(
      @Element KV<String, InspectContentResponse> element, MultiOutputReceiver out) {

    String filename = element.getKey();
    Long findings = Long.valueOf(element.getValue().getResult().getFindingsList().size());

    out.get(inspectSuccessTag).output(KV.of(filename, findings));

    for (String error : element.getValue().findInitializationErrors()) {
      LOG.info("Found Error in file {}, error: {}", filename, error.toString());
      out.get(inspectFailureTag).output(KV.of(filename, Long.valueOf(1)));
    }
  }
}
