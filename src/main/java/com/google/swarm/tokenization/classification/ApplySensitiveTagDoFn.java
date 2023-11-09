package com.google.swarm.tokenization.classification;

import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

import java.util.HashMap;
import java.util.Map;

public class ApplySensitiveTagDoFn extends DoFn<KV<String, Long>,PubsubMessage> {

    @ProcessElement
    public void processElement(ProcessContext c) {

        String filename = c.element().getKey();
        Long findings = c.element().getValue();
        Map<String,String> attributeMap = new HashMap<String,String>();
        if(findings > 0)
            attributeMap.put("DLPSensitiveTag", "SENSITIVE");
        else
            attributeMap.put("DLPSensitiveTag","NOT_SENSITIVE");

        c.output(new PubsubMessage(filename.getBytes(),attributeMap));

    }

}
