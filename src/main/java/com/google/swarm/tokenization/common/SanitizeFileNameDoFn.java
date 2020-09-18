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
import java.util.List;
import org.apache.beam.sdk.io.FileIO.ReadableFile;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Sanitizes input filenames by ensuring that the file extensions are valid and outputting keys that
 * are compatible with BigQuery table names.
 *
 * <p>TODO: Maybe the BigQuery table name sanitazation part should be moved to the BigQuery write
 * transforms instead.
 */
public class SanitizeFileNameDoFn extends DoFn<ReadableFile, KV<String, ReadableFile>> {

  public static final Logger LOG = LoggerFactory.getLogger(SanitizeFileNameDoFn.class);
  private static final List<String> ALLOWED_FILE_EXTENSIONS = Arrays.asList("csv", "avro");
  // Regular expression that matches valid BQ table IDs
  private static final String TABLE_REGEXP = "[-\\w$@]{1,1024}";

  public static String sanitizeFileName(ReadableFile file) {
    String fileName = file.getMetadata().resourceId().getFilename();
    String extension = Files.getFileExtension(fileName);
    String sanitizedName = fileName.replace(".", "_");
    sanitizedName = sanitizedName.replace("-", "_");
    String[] fileKey = sanitizedName.split("\\.", 2);

    if (!ALLOWED_FILE_EXTENSIONS.contains(extension) || !fileKey[0].matches(TABLE_REGEXP)) {
      throw new RuntimeException(
          "[Filename must contain a {csv,avro} extension. "
              + " BQ table name must contain only letters, numbers, or underscores ["
              + fileKey[1]
              + "], ["
              + fileKey[0]
              + "]");
    }

    // Return sanitized file name without extension
    return fileKey[0];
  }

  @ProcessElement
  public void processElement(ProcessContext c) {
    ReadableFile file = c.element();
    String fileName = sanitizeFileName(file);
    c.output(KV.of(fileName, file));
  }
}
