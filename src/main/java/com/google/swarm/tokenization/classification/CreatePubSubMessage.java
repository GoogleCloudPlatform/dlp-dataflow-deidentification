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

import java.util.HashMap;
import java.util.Map;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CreatePubSubMessage extends DoFn<KV<String, Long>, PubsubMessage> {

  public String messageType;
  public static final Logger LOG = LoggerFactory.getLogger(CreatePubSubMessage.class);

  public CreatePubSubMessage(String messageType) {
    this.messageType = messageType;
  }

  @ProcessElement
  public void processElement(ProcessContext c) {

    String filename = c.element().getKey();
    Map<String, String> attributeMap = new HashMap<String, String>();
    attributeMap.put("filename", filename);

    if (messageType.equals("success")) {
      Long findings = c.element().getValue();
      attributeMap.put("findings", findings.toString());
      if (findings > 0) attributeMap.put("DLPSensitiveTag", "SENSITIVE");
      else attributeMap.put("DLPSensitiveTag", "NOT_SENSITIVE");
      LOG.info("Inspect findings {} in file {}", findings.toString(), filename);
      c.output(new PubsubMessage(filename.getBytes(), attributeMap));

    } else {
      attributeMap.put("findings", "ERROR");
      LOG.info("Inspect errors in file {}", c.element().getValue().toString(), filename);
      c.output(new PubsubMessage("Error in DLP Inspect".getBytes(), attributeMap));
    }
  }
}
