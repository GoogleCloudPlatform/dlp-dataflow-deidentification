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

import com.google.common.io.Files;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.beam.sdk.io.FileIO.ReadableFile;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Sanitizes input filenames by ensuring that the file extensions are valid and outputting keys that
 * are compatible with BigQuery table names.
 */
public class SanitizeFileNameDoFn extends DoFn<ReadableFile, KV<String, ReadableFile>> {

  public static final Logger LOG = LoggerFactory.getLogger(SanitizeFileNameDoFn.class);
  private static final Set<String> ALLOWED_FILE_EXTENSIONS =
      Arrays.asList("csv", "avro", "jsonl", "txt").stream().collect(Collectors.toUnmodifiableSet());;
  // Regular expression that matches valid BQ table IDs
  private static final String TABLE_REGEXP = "[-\\w$@]{1,1024}";

  public static String sanitizeFileName(String file) {
    String extension = Files.getFileExtension(file);
    if (!ALLOWED_FILE_EXTENSIONS.contains(extension)) {
      throw new RuntimeException(
          "Invalid file name '"
              + file
              + "': must have one of these extensions: "
              + ALLOWED_FILE_EXTENSIONS);
    }

    String sanitizedName = file.substring(0, file.length() - extension.length() - 1);
    sanitizedName = sanitizedName.replace(".", "_");
    sanitizedName = sanitizedName.replace("-", "_");

    if (!sanitizedName.matches(TABLE_REGEXP)) {
      throw new RuntimeException(
          "Invalid file name '"
              + file
              + "': base name must be a valid BigQuery table name -"
              + " can contain only letters, numbers, or underscores");
    }

    // Return sanitized file name without extension
    return sanitizedName;
  }

  @ProcessElement
  public void processElement(ProcessContext c) {
    ReadableFile file = c.element();
    long lastModified = file.getMetadata().lastModifiedMillis();
    if (lastModified == 0L) {
      lastModified = Instant.now().getMillis();
    }
    String fileName = sanitizeFileName(file.getMetadata().resourceId().getFilename());
    c.outputWithTimestamp(KV.of(fileName, file), Instant.ofEpochMilli(lastModified));
  }
}
