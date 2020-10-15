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
package com.google.swarm.tokenization.txt;

import java.util.Arrays;
import java.util.Random;
import java.util.Scanner;
import java.util.stream.Collectors;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.swarm.tokenization.common.Util;

@SuppressWarnings("serial")
public class ParseTextLogDoFn extends DoFn<KV<String, String>, KV<String, String>> {

  private static final Logger LOG = LoggerFactory.getLogger(ParseTextLogDoFn.class);
  
  private Random r;

  @Setup
  public void setup() {
    r = new Random();
  }

  @ProcessElement
  public void processElement(ProcessContext c, MultiOutputReceiver out) {
    String fileName = c.element().getKey();
    String chatLog = c.element().getValue();
    String index = chatLog.substring(0, 1);
    String transcript = chatLog.substring(1);
    // escape header fields
    if (StringUtils.isNumeric(index)) {
      try (Scanner scanner = new Scanner(transcript.trim())) {
        scanner.useDelimiter("\\ ");
        int position = 1;
        StringBuilder agentTranscript = new StringBuilder();
        StringBuilder customerTranscript = new StringBuilder();
        boolean agentScript = true;
        while (scanner.hasNext()) {
          String word = scanner.next().trim();
          if (word.contains("[Customer]:")) {
            customerTranscript.append(StringUtils.remove(word, "[Customer]:"));
            agentScript = false;
            if (agentTranscript.length() > 0) {
              String[] tempValues = {
                String.format("%s%s%s%s%d", fileName, "_", index, "_", r.nextInt(100)),
                "agent",
                agentTranscript.toString(),
                String.valueOf(position),
                "N/A"
              };
              agentTranscript.setLength(0);
              position = position + 1;
              String finalTranscript =
                  Arrays.asList(tempValues).stream().collect(Collectors.joining(","));
              LOG.debug(finalTranscript);
              out.get(Util.agentTranscriptTuple).output(KV.of(fileName, finalTranscript));
            }

          } else if (word.contains("[Agent]:")) {
            agentTranscript.append(StringUtils.remove(word, "[Agent]:"));
            agentScript = true;
            if (customerTranscript.length() > 0) {
              String[] tempValues = {
                String.format("%s%s%s%s%d", fileName, "_", index, "_", r.nextInt(100)),
                "customer",
                customerTranscript.toString(),
                String.valueOf(position),
                "N/A"
              };
              customerTranscript.setLength(0);
              position = position + 1;
              String finalTranscript =
                  Arrays.asList(tempValues).stream().collect(Collectors.joining(","));
              LOG.debug(finalTranscript);
              out.get(Util.customerTranscriptTuple).output(KV.of(fileName, finalTranscript));
            }

          } else {
            if (agentScript) {
              agentTranscript.append(StringUtils.SPACE);
              agentTranscript.append(word);
            } else {
              customerTranscript.append(StringUtils.SPACE);
              customerTranscript.append(word);
            }
          }
        }
      }
    }
  }
}
