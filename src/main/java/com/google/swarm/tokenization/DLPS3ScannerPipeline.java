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

import com.google.swarm.tokenization.common.AudioInspectDataTransform;
import com.google.swarm.tokenization.common.BQWriteTransform;
import com.google.swarm.tokenization.common.DLPTransform;
import com.google.swarm.tokenization.common.FileReaderTransform;
import com.google.swarm.tokenization.common.S3ReaderOptions;
import com.google.swarm.tokenization.common.Util;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DLPS3ScannerPipeline {
  public static final Logger LOG = LoggerFactory.getLogger(DLPS3ScannerPipeline.class);

  public static void main(String[] args) {
    S3ReaderOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(S3ReaderOptions.class);
    run(options);
  }

  public static PipelineResult run(S3ReaderOptions options) {
    Pipeline p = Pipeline.create(options);

    PCollection<KV<String, String>> nonInspectedContents =
        p.apply(
            "File Read Transforrm",
            FileReaderTransform.newBuilder().setFilePattern(options.getFilePattern()).build());

    PCollectionTuple inspectedData =
        nonInspectedContents.apply(
            "DLPScanner",
            DLPTransform.newBuilder()
                .setInspectTemplateName(options.getInspectTemplateName())
                .setProjectId(options.getProject())
                .build());

    PCollection<Row> inspectedContents =
        inspectedData.get(Util.inspectData).setRowSchema(Util.bqDataSchema);

    PCollection<Row> inspectedStats =
        inspectedData.get(Util.auditData).setRowSchema(Util.bqAuditSchema);

    inspectedStats
        .apply("FileTrackerTransform", new AudioInspectDataTransform())
        .setRowSchema(Util.bqAuditSchema)
        .apply(
            "WriteAuditData",
            BQWriteTransform.newBuilder()
                .setTableSpec(options.getAuditTableSpec())
                .setMethod(options.getWriteMethod())
                .build());

    inspectedContents.apply(
        "WriteInspectData",
        BQWriteTransform.newBuilder()
            .setTableSpec(options.getTableSpec())
            .setMethod(options.getWriteMethod())
            .build());
    return p.run();
  }
}
