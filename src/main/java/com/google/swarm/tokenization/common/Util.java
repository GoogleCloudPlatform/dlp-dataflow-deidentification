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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.beam.sdk.schemas.Schema.toSchema;

import com.google.api.services.bigquery.model.TableCell;
import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.common.base.Charsets;
import com.google.gson.Gson;
import com.google.privacy.dlp.v2.InspectContentResponse;
import com.google.privacy.dlp.v2.Table;
import com.google.privacy.dlp.v2.Value;
import java.io.BufferedReader;
import java.io.IOException;
import java.net.URI;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import org.apache.beam.sdk.extensions.gcp.util.gcsfs.GcsPath;
import org.apache.beam.sdk.io.FileIO.ReadableFile;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.io.BaseEncoding;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.joda.time.DateTimeZone;
import org.joda.time.Instant;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.DateTimeFormatterBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("serial")
public class Util {

  public static final Logger LOG = LoggerFactory.getLogger(Util.class);

  public enum DLPMethod {
    INSPECT,
    DEID,
    REID
  }

  public enum FileType {
    CSV,
    AVRO,
    JSON
  }

  public static final Gson gson = new Gson();
  private static final char DEFAULT_SEPARATOR = ',';
  private static final char DEFAULT_QUOTE = '"';

  private static final DateTimeFormatter BIGQUERY_TIMESTAMP_PRINTER;
  public static final TupleTag<KV<String, String>> contentTag =
      new TupleTag<KV<String, String>>() {};
  public static final TupleTag<KV<String, ReadableFile>> headerTag =
      new TupleTag<KV<String, ReadableFile>>() {};

  public static final TupleTag<KV<String, TableRow>> inspectOrDeidSuccess =
      new TupleTag<KV<String, TableRow>>() {};
  public static final TupleTag<KV<String, TableRow>> inspectOrDeidFailure =
      new TupleTag<KV<String, TableRow>>() {};

  public static final TupleTag<KV<String, TableRow>> reidSuccess =
      new TupleTag<KV<String, TableRow>>() {};
  public static final TupleTag<KV<String, TableRow>> reidFailure =
      new TupleTag<KV<String, TableRow>>() {};

  public static final TupleTag<KV<String, InspectContentResponse>> inspectApiCallSuccess =
      new TupleTag<KV<String, InspectContentResponse>>() {};
  public static final TupleTag<KV<String, TableRow>> inspectApiCallError =
      new TupleTag<KV<String, TableRow>>() {};

  public static final String BQ_DLP_INSPECT_TABLE_NAME = String.valueOf("dlp_inspection_result");
  public static final String BQ_ERROR_TABLE_NAME = String.valueOf("error_log");
  public static final String BQ_REID_TABLE_EXT = String.valueOf("re_id");

  public static final DateTimeFormatter TIMESTAMP_FORMATTER =
      DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSSSSS");

  public static Table.Row convertCsvRowToTableRow(String row) {
    String[] values = row.split(",");
    Table.Row.Builder tableRowBuilder = Table.Row.newBuilder();
    for (String value : values) {
      tableRowBuilder.addValues(Value.newBuilder().setStringValue(value).build());
    }

    return tableRowBuilder.build();
  }

  public static String checkHeaderName(String name) {
    String checkedHeader = name.replaceAll("\\s", "_");
    checkedHeader = checkedHeader.replaceAll("'", "");
    checkedHeader = checkedHeader.replaceAll("/", "");
    checkedHeader = checkedHeader.replaceAll("\\W", "");
    LOG.debug("Name {} checkedHeader {}", name, checkedHeader);
    return checkedHeader;
  }

  public static final Schema dlpInspectionSchema =
      Stream.of(
              Schema.Field.of("job_name", FieldType.STRING).withNullable(true),
              Schema.Field.of("source_file", FieldType.STRING).withNullable(true),
              Schema.Field.of("transaction_time", FieldType.STRING).withNullable(true),
              Schema.Field.of("quote", FieldType.STRING).withNullable(true),
              Schema.Field.of("info_type_name", FieldType.STRING).withNullable(true),
              Schema.Field.of("likelihood", FieldType.STRING).withNullable(true),
              Schema.Field.of("location_start_byte_range", FieldType.INT64).withNullable(true),
              Schema.Field.of("location_end_byte_range", FieldType.INT64).withNullable(true),
              Schema.Field.of("record_loc_field_id", FieldType.STRING).withNullable(true))
          .collect(toSchema());

  public static final Schema errorSchema =
      Stream.of(
              Schema.Field.of("file_name", FieldType.STRING).withNullable(true),
              Schema.Field.of("transaction_timestamp", FieldType.STRING).withNullable(true),
              Schema.Field.of("error_messagee", FieldType.STRING).withNullable(true),
              Schema.Field.of("stack_trace", FieldType.STRING).withNullable(true))
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

  public static boolean isDefaultMode(String[] args) {

    for (String arg : args) {
      String[] splitFromEqual = arg.split("=");
      String value = splitFromEqual[1];
      if (value.equals("s3")) {
        return false;
      }
    }
    return true;
  }

  static {
    DateTimeFormatter dateTimePart =
        new DateTimeFormatterBuilder()
            .appendYear(4, 4)
            .appendLiteral('-')
            .appendMonthOfYear(2)
            .appendLiteral('-')
            .appendDayOfMonth(2)
            .appendLiteral(' ')
            .appendHourOfDay(2)
            .appendLiteral(':')
            .appendMinuteOfHour(2)
            .appendLiteral(':')
            .appendSecondOfMinute(2)
            .toFormatter()
            .withZoneUTC();
    BIGQUERY_TIMESTAMP_PRINTER =
        new DateTimeFormatterBuilder()
            .append(dateTimePart)
            .appendLiteral('.')
            .appendFractionOfSecond(3, 3)
            .appendLiteral(" UTC")
            .toFormatter();
  }

  private static Object fromBeamField(FieldType fieldType, Object fieldValue) {
    if (fieldValue == null) {
      if (!fieldType.getNullable()) {
        throw new IllegalArgumentException("Field is not nullable.");
      }
      return null;
    }
    switch (fieldType.getTypeName()) {
      case ARRAY:
      case ITERABLE:
        FieldType elementType = fieldType.getCollectionElementType();
        Iterable<?> items = (Iterable<?>) fieldValue;
        List<Object> convertedItems = Lists.newArrayListWithCapacity(Iterables.size(items));
        for (Object item : items) {
          convertedItems.add(fromBeamField(elementType, item));
        }
        return convertedItems;
      case ROW:
        return toTableRow((Row) fieldValue);
      case DATETIME:
        return ((Instant) fieldValue)
            .toDateTime(DateTimeZone.UTC)
            .toString(BIGQUERY_TIMESTAMP_PRINTER);
      case INT16:
      case INT32:
      case INT64:
      case FLOAT:
      case DOUBLE:
      case STRING:
      case BOOLEAN:
        return fieldValue.toString();
      case DECIMAL:
        return fieldValue.toString();
      case BYTES:
        return BaseEncoding.base64().encode((byte[]) fieldValue);
      default:
        return fieldValue;
    }
  }

  public static TableRow toTableRow(Row row) {
    TableRow output = new TableRow();
    for (int i = 0; i < row.getFieldCount(); i++) {
      Object value = row.getValue(i);
      Field schemaField = row.getSchema().getField(i);
      output = output.set(schemaField.getName(), fromBeamField(schemaField.getType(), value));
    }
    return output;
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

  @SuppressWarnings("null")
  public static List<String> parseLine(String cvsLine, char separators, char customQuote) {

    List<String> result = new ArrayList<>();

    // if empty, return!
    if (cvsLine == null && cvsLine.isEmpty()) {
      return result;
    }

    if (customQuote == ' ') {
      customQuote = DEFAULT_QUOTE;
    }

    if (separators == ' ') {
      separators = DEFAULT_SEPARATOR;
    }

    StringBuffer curVal = new StringBuffer();
    boolean inQuotes = false;
    boolean startCollectChar = false;
    boolean doubleQuotesInColumn = false;

    char[] chars = cvsLine.toCharArray();

    for (char ch : chars) {

      if (inQuotes) {
        startCollectChar = true;
        if (ch == customQuote) {
          inQuotes = false;
          doubleQuotesInColumn = false;
        } else {

          // Fixed : allow "" in custom quote enclosed
          if (ch == '\"') {
            if (!doubleQuotesInColumn) {
              curVal.append(ch);
              doubleQuotesInColumn = true;
            }
          } else {
            curVal.append(ch);
          }
        }
      } else {
        if (ch == customQuote) {

          inQuotes = true;

          // Fixed : allow "" in empty quote enclosed
          if (chars[0] != '"' && customQuote == '\"') {
            curVal.append('"');
          }

          // double quotes in column will hit this!
          if (startCollectChar) {
            curVal.append('"');
          }

        } else if (ch == separators) {

          result.add(curVal.toString());

          curVal = new StringBuffer();
          startCollectChar = false;

        } else if (ch == '\r') {
          // ignore LF characters
          continue;
        } else if (ch == '\n') {
          // the end, break!
          break;
        } else {
          curVal.append(ch);
        }
      }
    }

    result.add(curVal.toString());

    return result;
  }

  public static TableRow createBqRow(Table.Row tokenizedValue, String[] headers) {
    TableRow bqRow = new TableRow();
    AtomicInteger headerIndex = new AtomicInteger(0);
    List<TableCell> cells = new ArrayList<>();
    tokenizedValue
        .getValuesList()
        .forEach(
            value -> {
              String checkedHeaderName =
                  Util.checkHeaderName(headers[headerIndex.getAndIncrement()].toString());
              bqRow.set(checkedHeaderName, value.getStringValue());
              cells.add(new TableCell().set(checkedHeaderName, value.getStringValue()));
            });
    bqRow.setF(cells);
    return bqRow;
  }

  public static String getQueryFromGcs(String gcsPath) {
    GcsPath path = GcsPath.fromUri(URI.create(gcsPath));
    Storage storage = StorageOptions.getDefaultInstance().getService();
    BlobId blobId = BlobId.of(path.getBucket(), path.getObject());
    byte[] content = storage.readAllBytes(blobId);
    String contentString = new String(content, UTF_8);
    LOG.debug("Query: {}", contentString);
    return contentString;
  }
}
