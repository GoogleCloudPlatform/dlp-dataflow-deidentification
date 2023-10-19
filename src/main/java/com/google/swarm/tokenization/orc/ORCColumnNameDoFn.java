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
package com.google.swarm.tokenization.orc;

import java.io.IOException;
import java.util.List;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.orc.Reader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ORCColumnNameDoFn
    extends DoFn<KV<String, FileIO.ReadableFile>, KV<String, List<String>>> {

  public static final Logger LOG = LoggerFactory.getLogger(ORCColumnNameDoFn.class);

  public String projectId;

  public ORCColumnNameDoFn(String projectId) {
    this.projectId = projectId;
  }

  @ProcessElement
  public void processElement(ProcessContext c) throws IOException {
    FileIO.ReadableFile orcFile = c.element().getValue();
    String fileName = c.element().getKey();
    String filePath = orcFile.getMetadata().resourceId().toString();

    Reader reader = new ORCFileReader().createORCFileReader(filePath, projectId);
    List<String> fieldNames = reader.getSchema().getFieldNames();

    c.output(KV.of(fileName, fieldNames));
  }
}
