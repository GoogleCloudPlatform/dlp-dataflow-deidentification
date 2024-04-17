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

import com.google.auto.value.AutoValue;
import com.google.privacy.dlp.v2.InspectContentResponse;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.*;

@AutoValue
public abstract class ClassifyFiles
    extends PTransform<PCollection<KV<String, InspectContentResponse>>, PDone> {

  public abstract String outputPubSubTopic();

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder setOutputPubSubTopic(String value);

    public abstract ClassifyFiles build();
  }

  public static ClassifyFiles.Builder newBuilder() {
    return new AutoValue_ClassifyFiles.Builder();
  }

  @Override
  public PDone expand(PCollection<KV<String, InspectContentResponse>> input) {

    PCollectionTuple findings =
        input.apply(
            "ProcessFindings",
            ParDo.of(new ProcessInspectFindingsDoFn())
                .withOutputTags(
                    ProcessInspectFindingsDoFn.getInspectSuccessTag(),
                    TupleTagList.of(ProcessInspectFindingsDoFn.getInspectFailureTag())));

    PCollection<PubsubMessage> inspectSuccess =
        findings
            .get(ProcessInspectFindingsDoFn.getInspectSuccessTag())
            .apply("FindTotalFindings", Combine.<String, Long, Long>perKey(Sum.ofLongs()))
            .apply("ApplySensitiveTag", ParDo.of(new CreatePubSubMessage("success")));

    PCollection<PubsubMessage> inspectFailure =
        findings
            .get(ProcessInspectFindingsDoFn.getInspectFailureTag())
            .apply("FindTotalErrors", Combine.<String, Long, Long>perKey(Sum.ofLongs()))
            .apply("CreateMessage", ParDo.of(new CreatePubSubMessage("error")));

    return PCollectionList.of(inspectSuccess)
        .and(inspectFailure)
        .apply("Flatten", Flatten.pCollections())
        .apply("PublishToPubSub", PubsubIO.writeMessages().to(outputPubSubTopic()));
  }
}
