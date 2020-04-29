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

import static org.apache.beam.sdk.schemas.Schema.toSchema;

import com.google.api.client.util.Charsets;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.api.services.storage.Storage;
import com.google.privacy.dlp.v2.FieldId;
import com.google.privacy.dlp.v2.Table;
import com.google.privacy.dlp.v2.Value;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.apache.beam.sdk.io.FileIO.ReadableFile;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang.StringUtils;
import org.joda.time.DateTimeZone;
import org.joda.time.Instant;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Util {

  public static final Logger LOG = LoggerFactory.getLogger(Util.class);
  private static final String TABLE_REGEXP = "[-\\w$@]{1,1024}";
  public static Integer DLP_PAYLOAD_LIMIT = 52400;
  public static final String BQ_TABLE_NAME = String.valueOf("dlp_s3_inspection_result");
  public static final DateTimeFormatter TIMESTAMP_FORMATTER =
      DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSSSSS");
  private static final String NESTED_SCHEMA_REGEX = ".*[^=]=(.*[^ ]), .*[^=]=(.*[^ ])";
  private static final String ALLOWED_FILE_EXTENSION = String.valueOf("csv");
  public static final String ALLOWE_NOTIFICSTION_EVET_TYPE = String.valueOf("OBJECT_FINALIZE");
  public static String INSPECTED = "INSPECTED";
  public static String FAILED = "FAILED";
  public static TupleTag<Row> inspectData = new TupleTag<Row>() {};
  public static TupleTag<Row> auditData = new TupleTag<Row>() {};
  public static TupleTag<Row> errorData = new TupleTag<Row>() {};

  public static String parseBucketName(String value) {
    return value.substring(5, value.length() - 1);
  }

  public static Table.Row convertCsvRowToTableRow(String row) {
    String[] values = row.split(",");
    Table.Row.Builder tableRowBuilder = Table.Row.newBuilder();
    for (String value : values) {
      tableRowBuilder.addValues(Value.newBuilder().setStringValue(value).build());
    }

    return tableRowBuilder.build();
  }

  public static Table createDLPTable(List<FieldId> headers, List<String> lines) {

    List<Table.Row> rows = new ArrayList<>();
    lines.forEach(
        line -> {
          rows.add(convertCsvRowToTableRow(line));
        });
    Table table = Table.newBuilder().addAllHeaders(headers).addAllRows(rows).build();

    return table;
  }

  public static boolean findEncryptionType(
      String keyRing, String keyName, String csek, String csekhash) {

    return keyRing != null || keyName != null || csek != null || csekhash != null;
  }

  public static BufferedReader getReader(
      boolean customerSuppliedKey,
      String objectName,
      String bucketName,
      ReadableFile file,
      String key,
      ValueProvider<String> csekhash) {

    BufferedReader br = null;

    try {
      if (!customerSuppliedKey) {
        ReadableByteChannel channel = file.openSeekable();
        br = new BufferedReader(Channels.newReader(channel, Charsets.ISO_8859_1.name()));
      } else {

        Storage storage = null;
        InputStream objectData = null;
        try {
          storage = StorageFactory.getService();
        } catch (GeneralSecurityException e) {
          LOG.error("Error Creating Storage API Client");
          e.printStackTrace();
        }
        try {
          objectData =
              StorageFactory.downloadObject(storage, bucketName, objectName, key, csekhash.get());
        } catch (Exception e) {
          LOG.error("Error Reading the Encrypted File in GCS- Customer Supplied Key");
          e.printStackTrace();
        }

        br = new BufferedReader(new InputStreamReader(objectData));
      }

    } catch (IOException e) {
      LOG.error("Error Reading the File " + e.getMessage());
      e.printStackTrace();
      System.exit(1);
    }

    return br;
  }

  public static String checkHeaderName(String name) {
    String checkedHeader = name.replaceAll("\\s", "_");
    checkedHeader = checkedHeader.replaceAll("'", "");
    checkedHeader = checkedHeader.replaceAll("/", "");
    checkedHeader = checkedHeader.replaceAll("\\W", "");
    LOG.debug("Name {} checkedHeader {}", name, checkedHeader);
    return checkedHeader;
  }

  @SuppressWarnings("serial")
  public static TableSchema getSchema(List<String> outputHeaders) {
    return new TableSchema()
        .setFields(
            new ArrayList<TableFieldSchema>() {

              {
                outputHeaders.forEach(
                    header -> {
                      add(new TableFieldSchema().setName(header).setType("STRING"));
                    });
              }
            });
  }

  private static boolean isTimestamp(String value) {
    try {
      Instant.parse(value);
      return true;
    } catch (IllegalArgumentException e) {
      return false;
    }
  }

  public static boolean isNumeric(String value) {

    if (StringUtils.isNumeric(value)) {
      return true;
    }
    try {
      Float.parseFloat(value);
      return true;
    } catch (NumberFormatException e) {
      return false;
    }
  }

  public static String typeCheck(String value) {

    if (value == null || value.isEmpty()) {
      return "String";
    }
    if (isNumeric(value)) {
      return "FLOAT";
    } else if (isTimestamp(value)) {
      return "TIMESTAMP";

    } else if (value.matches(NESTED_SCHEMA_REGEX)) {
      return "RECORD";
    } else {
      return "STRING";
    }
  }

  public static final Schema dlpInspectionSchema =
      Stream.of(
              Schema.Field.of("source_file", FieldType.STRING).withNullable(true),
              Schema.Field.of("bytes_inspected", FieldType.INT64).withNullable(true),
              Schema.Field.of("transaction_time", FieldType.STRING).withNullable(true),
              Schema.Field.of("info_type_name", FieldType.STRING).withNullable(true),
              Schema.Field.of("likelihood", FieldType.STRING).withNullable(true),
              Schema.Field.of("quote", FieldType.STRING).withNullable(true),
              Schema.Field.of("location_start_byte_range", FieldType.INT64).withNullable(true),
              Schema.Field.of("location_end_byte_range", FieldType.INT64).withNullable(true))
          .collect(toSchema());

  public static final Schema bqDataSchema =
      Stream.of(
              Schema.Field.of("source_file", FieldType.STRING).withNullable(true),
              Schema.Field.of("transaction_time", FieldType.STRING).withNullable(true),
              Schema.Field.of("info_type_name", FieldType.STRING).withNullable(true),
              Schema.Field.of("likelihood", FieldType.STRING).withNullable(true),
              Schema.Field.of("quote", FieldType.STRING).withNullable(true),
              Schema.Field.of("location_start_byte_range", FieldType.INT64).withNullable(true),
              Schema.Field.of("location_end_byte_range", FieldType.INT64).withNullable(true))
          .collect(toSchema());
  public static final Schema bqAuditSchema =
      Stream.of(
              Schema.Field.of("source_file", FieldType.STRING).withNullable(true),
              Schema.Field.of("transaction_time", FieldType.STRING).withNullable(true),
              Schema.Field.of("total_bytes_inspected", FieldType.INT64).withNullable(true),
              Schema.Field.of("status", FieldType.STRING).withNullable(true))
          .collect(toSchema());
  public static final Schema errorSchema =
      Stream.of(
              Schema.Field.of("source_file", FieldType.STRING).withNullable(true),
              Schema.Field.of("transaction_time", FieldType.STRING).withNullable(true),
              Schema.Field.of("error_message", FieldType.STRING).withNullable(true))
          .collect(toSchema());

  public static String getTimeStamp() {
    return TIMESTAMP_FORMATTER.print(Instant.now().toDateTime(DateTimeZone.UTC));
  }

  public static BufferedReader getReader(ReadableFile csvFile) {
    BufferedReader br = null;
    ReadableByteChannel channel = null;
    /** read the file and create buffered reader */
    try {
      channel = csvFile.openSeekable();

    } catch (IOException e) {
      LOG.error("Failed to Read File {}", e.getMessage());
      throw new RuntimeException(e);
    }

    if (channel != null) {

      br = new BufferedReader(Channels.newReader(channel, Charsets.ISO_8859_1.name()));
    }

    return br;
  }

  public static List<String> getFileHeaders(BufferedReader reader) {
    List<String> headers = new ArrayList<>();
    try {
      CSVRecord csvHeader = CSVFormat.DEFAULT.parse(reader).getRecords().get(0);
      csvHeader.forEach(
          headerValue -> {
            headers.add(headerValue);
          });
    } catch (IOException e) {
      LOG.error("Failed to get csv header values}", e.getMessage());
      throw new RuntimeException(e);
    }
    return headers;
  }

  public static String getFileName(ReadableFile file) {
    String csvFileName = file.getMetadata().resourceId().getFilename().toString();
    /** taking out .csv extension from file name e.g fileName.csv->fileName */
    String[] fileKey = csvFileName.split("\\.", 2);

    if (!fileKey[1].equals(ALLOWED_FILE_EXTENSION) || !fileKey[0].matches(TABLE_REGEXP)) {
      throw new RuntimeException(
          "[Filename must contain a CSV extension "
              + " BQ table name must contain only letters, numbers, or underscores ["
              + fileKey[1]
              + "], ["
              + fileKey[0]
              + "]");
    }
    /** returning file name without extension */
    return fileKey[0];
  }
}
