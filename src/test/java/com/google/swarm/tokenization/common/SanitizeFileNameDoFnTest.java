package com.google.swarm.tokenization.common;

import static org.junit.Assert.*;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

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
      assertTrue(e.getMessage().startsWith(
          "Invalid file name 'no-extention-file': must have one of these extensions: "));
    }
  }

  @Test
  public void sanitizeFileNameInvalidBaseName() {
    try {
      SanitizeFileNameDoFn.sanitizeFileName("invalid-table-name^.csv");
    } catch (RuntimeException e) {
      assertEquals("Expected error message:",
          "Invalid file name 'invalid-table-name^.csv': base name must be a valid BigQuery "
              + "table name - can contain only letters, numbers, or underscores",
          e.getMessage());
    }
  }
}