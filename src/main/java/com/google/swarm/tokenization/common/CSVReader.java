/*
 * Copyright 2018 Google LLC
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

import com.google.swarm.tokenization.CSVStreamingPipeline;
import java.io.BufferedReader;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.beam.sdk.io.FileIO.ReadableFile;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("serial")
public class CSVReader extends DoFn<ReadableFile, KV<String, List<String>>> {
  public static final Logger LOG = LoggerFactory.getLogger(CSVStreamingPipeline.class);
  private ValueProvider<String> csek;
  private ValueProvider<String> csekHash;
  private ValueProvider<String> fileDecryptKeyName;
  private ValueProvider<String> fileDecryptKey;
  private String projectId;
  private ValueProvider<Integer> batchSize;

  public CSVReader(
      ValueProvider<String> csek,
      ValueProvider<String> csekHash,
      ValueProvider<String> fileDecryptKeyName,
      ValueProvider<String> fileDecryptKey,
      String projectId,
      ValueProvider<Integer> batchSize) {
    this.csek = csek;
    this.csekHash = csekHash;
    this.fileDecryptKeyName = fileDecryptKeyName;
    this.fileDecryptKey = fileDecryptKey;
    this.projectId = projectId;
    this.batchSize = batchSize;
  }

  private static <T> Collection<List<T>> partition(List<T> list, int size) {
    final AtomicInteger counter = new AtomicInteger(0);

    return list.stream()
        .collect(Collectors.groupingBy(it -> counter.getAndIncrement() / size))
        .values();
  }

  @ProcessElement
  public void processElement(ProcessContext c) {
    ReadableFile f = c.element();
    boolean customerSuppliedKey = false;
    String key = null;
    BufferedReader br = null;
    List<String> contents = new ArrayList<>();

    if (this.csek.isAccessible()) {

      customerSuppliedKey =
          Util.findEncryptionType(
              this.fileDecryptKeyName.get(),
              this.fileDecryptKey.get(),
              this.csek.get(),
              this.csekHash.get());
    }

    if (customerSuppliedKey) {

      try {
        key =
            KMSFactory.decrypt(
                this.projectId,
                "global",
                this.fileDecryptKeyName.get(),
                this.fileDecryptKey.get(),
                this.csek.get());
      } catch (IOException e) {

        e.printStackTrace();
      } catch (GeneralSecurityException e) {

        e.printStackTrace();
      }
    }
    String bucketName =
        Util.parseBucketName(f.getMetadata().resourceId().getCurrentDirectory().toString());

    String objectName = f.getMetadata().resourceId().getFilename().toString();
    String[] fileKey = objectName.split("\\.", 2);
    br = Util.getReader(customerSuppliedKey, objectName, bucketName, f, key, this.csekHash);
    contents = br.lines().collect(Collectors.toList());
    String header = contents.get(0);
    LOG.debug("File Size {}, Header{}", contents.size(), header.toString());
    Collection<List<String>> multiContents =
        partition(
            contents.stream().skip(1).collect(Collectors.toList()),
            this.batchSize.get().intValue() * 3);
    LOG.info("Number of Sub Lists {}", multiContents.size());
    multiContents.forEach(
        content -> {
          content.add(0, header);
          LOG.info("Content Size {}", content.size());
          c.output(KV.of(fileKey[0].toString(), content));
        });
  }
}
