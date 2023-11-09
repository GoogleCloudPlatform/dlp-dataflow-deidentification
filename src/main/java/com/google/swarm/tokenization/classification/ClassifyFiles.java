package com.google.swarm.tokenization.classification;

import com.google.auto.value.AutoValue;
import com.google.privacy.dlp.v2.InspectContentResponse;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
@AutoValue
public abstract class ClassifyFiles extends PTransform<PCollection<KV<String, InspectContentResponse>>, PDone> {


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

     return input.apply("CountFindings",ParDo.of(new DoFn<KV<String, InspectContentResponse>, KV<String, Long>>() {
            @ProcessElement
            public void processElement(ProcessContext c) {
                String filename = c.element().getKey();
                Long findings = Long.valueOf(c.element().getValue().getResult()
                        .getFindingsList().size());

                c.output(KV.of(filename,findings));
            }
        })).apply("FindTotalFindings", Combine.<String,Long,Long>perKey(Sum.ofLongs()))
           .apply("ApplySensitiveTag", ParDo.of(new ApplySensitiveTagDoFn()))
           .apply("PublishToPubSub", PubsubIO.writeMessages().to(outputPubSubTopic()));

    }
}