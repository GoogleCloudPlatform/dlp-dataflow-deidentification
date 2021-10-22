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

import static org.junit.Assert.*;

import org.junit.Test;

public class SanitizeFileNameDoFnTest {

  @Test
  public void sanitizeFileNameValid() {
    String result = SanitizeFileNameDoFn.sanitizeFileName("valid-file.csv");
    assertEquals("Sanitized name", "valid_file", result);
  }

  @Test
  public void sanitizeFileNameNoExtension() {
    try {
      SanitizeFileNameDoFn.sanitizeFileName("no-extention-file");
    } catch (RuntimeException e) {
      assertTrue(
          e.getMessage()
              .startsWith(
                  "Invalid file name 'no-extention-file': must have one of these extensions: "));
    }
  }

  @Test
  public void sanitizeFileNameInvalidBaseName() {
    try {
      SanitizeFileNameDoFn.sanitizeFileName("invalid-table-name^.csv");
    } catch (RuntimeException e) {
      assertEquals(
          "Expected error message:",
          "Invalid file name 'invalid-table-name^.csv': base name must be a valid BigQuery "
              + "table name - can contain only letters, numbers, or underscores",
          e.getMessage());
    }
  }
}
