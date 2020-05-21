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
package com.google.swarm.tokenization.common;

import org.apache.beam.sdk.io.FileIO.ReadableFile;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileSourceDoFn extends DoFn<ReadableFile, KV<String, ReadableFile>> {
  public static final Logger LOG = LoggerFactory.getLogger(FileSourceDoFn.class);

  private final Counter numberOfFilesPassedValidation =
          Metrics.counter(FileSourceDoFn.class, "NumberOfFilesPassedValidation");

  @ProcessElement
  public void processElement(ProcessContext c) {
    ReadableFile file = c.element();
    String fileName = file.getMetadata().resourceId().toString();
    String key = String.format("%s|%s", fileName, Instant.now().getMillis());
    LOG.info("File Read Transform:AddFileNameAsKey: {} is as added as a key for the file {}. ",key,fileName);
    numberOfFilesPassedValidation.inc(1L);
    c.output(KV.of(key, file));
  }
}

