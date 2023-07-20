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
package com.google.swarm.tokenization.common;

import com.google.common.io.Files;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.io.FileSystems;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.regex.Pattern;
import java.io.IOException;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.io.fs.MatchResult.Metadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PubSubReadFileMetadataDoFn
    extends DoFn<PubsubMessage, Metadata> {
    public static final Logger LOG = LoggerFactory.getLogger(PubSubReadFileMetadataDoFn.class);
    private String filePattern;

    public PubSubReadFileMetadataDoFn(String pattern) {
        this.filePattern = pattern;
    }

    public static boolean matches(String gsUrl, String pattern) {
        String regex = pattern
                .replace(".", "\\.")
                .replace("*", ".*")
                .replace("?", ".");
        Pattern compiledPattern = Pattern.compile(regex);
        return compiledPattern.matcher(gsUrl).matches();
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
        PubsubMessage message = c.element();
        String filename  = message.getAttribute("objectId");
        String bucketId  = message.getAttribute("bucketId");
        String extension = Files.getFileExtension(filename);

        String gsUrl = "gs://" + bucketId + "/" + filename;
        LOG.info("Received " + filename + " " + bucketId + " " + extension + " " + gsUrl + " " + this.filePattern);

        try {
            if (filename.endsWith("/") || !Util.ALLOWED_FILE_EXTENSIONS.contains(extension)
                || !matches(gsUrl, this.filePattern)) return;
            LOG.info("Processing " + filename + " " + bucketId + " " + extension);
            Metadata fileMetadata = FileSystems.matchSingleFileSpec(gsUrl);
            c.output(fileMetadata);
        } catch (IOException e) {
            LOG.error("GCS Failure retrieving {}: {}", gsUrl, e);
        }
    }
  }
