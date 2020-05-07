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
package com.google.swarm.tokenization;

import com.google.swarm.tokenization.common.AuditInspectDataTransform;
import com.google.swarm.tokenization.common.DLPTransform;
import com.google.swarm.tokenization.common.FileReaderTransform;
import com.google.swarm.tokenization.common.RowToJson;
import com.google.swarm.tokenization.common.S3ReaderOptions;
import com.google.swarm.tokenization.common.Util;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DLPS3ScannerPipeline {
  public static final Logger LOG = LoggerFactory.getLogger(DLPS3ScannerPipeline.class);

  public static void main(String[] args) {
    S3ReaderOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(S3ReaderOptions.class);
    // options.setEnableStreamingEngine(true);
    run(options);
  }

  public static PipelineResult run(S3ReaderOptions options) {
    Pipeline p = Pipeline.create(options);

    PCollection<KV<String, String>> nonInspectedContents =
        p.apply(
                "File Read Transform",
                FileReaderTransform.newBuilder()
                    .setSubscriber(options.getSubscriber())
                    .setDelimeter(options.getDelimeter())
                    .setKeyRange(options.getKeyRange())
                    .build())
            .apply(
                "Fixed Window",
                Window.<KV<String, String>>into(FixedWindows.of(Duration.standardSeconds(1)))
                    .triggering(
                        AfterProcessingTime.pastFirstElementInPane().plusDelayOf(Duration.ZERO))
                    .discardingFiredPanes()
                    .withAllowedLateness(Duration.ZERO));

    //    nonInspectedContents.apply("Print", ParDo.of(new DoFn<KV<String,String>, String>(){
    //
    //    		@ProcessElement
    //    		public void processElement(ProcessContext c) {
    //    			c.output(c.element().getValue());
    //    		}
    //    }));

    PCollectionTuple inspectedData =
        nonInspectedContents.apply(
            "DLPScanner",
            DLPTransform.newBuilder()
                .setInspectTemplateName(options.getInspectTemplateName())
                .setProjectId(options.getProject())
                .setBatchSize(options.getBatchSize())
                .build());

    PCollection<Row> inspectedContents =
        inspectedData.get(Util.inspectData).setRowSchema(Util.bqDataSchema);

    PCollection<Row> inspectedStats =
        inspectedData.get(Util.auditData).setRowSchema(Util.bqAuditSchema);

    PCollection<Row> auditData =
        inspectedStats
            .apply("FileTrackerTransform", new AuditInspectDataTransform())
            .setRowSchema(Util.bqAuditSchema);

    auditData.apply(
        "WriteAuditData",
        BigQueryIO.<Row>write()
            .to(options.getAuditTableSpec())
            .withMethod(BigQueryIO.Write.Method.STREAMING_INSERTS)
            .useBeamSchema()
            .withoutValidation()
            .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
            .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER));

    auditData
        .apply("RowToJson", new RowToJson())
        .apply("WriteToTopic", PubsubIO.writeStrings().to(options.getTopic()));

    //    inspectedContents.apply(
    //        "WriteInspectData",
    //        BQWriteTransform.newBuilder()
    //            .setTableSpec(options.getTableSpec())
    //            .setMethod(options.getWriteMethod())
    //            .build());
    return p.run();
  }
}
