/*
 * Copyright 2024 Google LLC
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
package com.google.swarm.tokenization.transforms;

import com.google.auto.value.AutoValue;
import com.google.privacy.dlp.v2.Table;
import com.google.swarm.tokenization.common.Util;
import java.util.List;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@AutoValue
public abstract class ProcessFiles
    extends PTransform<PCollection<KV<String, FileIO.ReadableFile>>, PCollectionTuple> {

  public abstract List<Util.FileType> fileTypes();

  public static final Logger LOG = LoggerFactory.getLogger(ProcessFiles.class);

  public static final TupleTag<KV<String, Table.Row>> tableRows =
      new TupleTag<KV<String, Table.Row>>() {};
  public static final TupleTag<KV<String, List<String>>> headersMap =
      new TupleTag<KV<String, List<String>>>() {};

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder setFileTypes(List<Util.FileType> value);

    public abstract ProcessFiles build();
  }

  public static ProcessFiles.Builder newBuilder() {
    return new AutoValue_ProcessFiles.Builder();
  }

  @Override
  public PCollectionTuple expand(PCollection<KV<String, FileIO.ReadableFile>> input) {

    PCollectionList<KV<String, FileIO.ReadableFile>> fileList =
        input.apply(
            Partition.of(
                Util.FileType.values().length,
                new Partition.PartitionFn<KV<String, FileIO.ReadableFile>>() {
                  @Override
                  public int partitionFor(KV<String, FileIO.ReadableFile> elem, int numPartitions) {
                    String resourceId = String.valueOf(elem.getValue().getMetadata().resourceId());
                    String extension =
                        String.valueOf(resourceId.substring(resourceId.lastIndexOf(".") + 1));
                    LOG.info(
                        "Found extension: {} and FileType: {} ",
                        extension,
                        Util.FileType.valueOf(extension.toUpperCase()).toString());
                    return Util.FileType.valueOf(extension.toUpperCase()).ordinal();
                  }
                }));

    PCollectionList<KV<String, Table.Row>> recordsList = PCollectionList.empty(input.getPipeline());
    PCollectionList<KV<String, List<String>>> headerList =
        PCollectionList.empty(input.getPipeline());

    for (Util.FileType fileType : fileTypes()) {
      switch (fileType) {
        case AVRO:
          PCollection<KV<String, FileIO.ReadableFile>> inputFilesAvro =
              fileList.get(Util.FileType.AVRO.ordinal());
          PCollectionTuple avroRecords =
              inputFilesAvro.apply("ProcessAVRO", ProcessAvro.newBuilder().build());
          recordsList = recordsList.and(avroRecords.get(ProcessAvro.recordsTuple));
          headerList = headerList.and(avroRecords.get(ProcessAvro.headersTuple));
          break;

        case CSV:
          PCollection<KV<String, FileIO.ReadableFile>> inputFilesCsv =
              fileList.get(Util.FileType.CSV.ordinal());
          PCollectionTuple csvRecords =
              inputFilesCsv.apply("ProcessCSV", ProcessCSV.newBuilder().build());
          recordsList = recordsList.and(csvRecords.get(ProcessCSV.recordsTuple));
          headerList = headerList.and(csvRecords.get(ProcessCSV.headersTuple));
          break;

        case JSONL:
          PCollection<KV<String, FileIO.ReadableFile>> inputFilesJsonl =
              fileList.get(Util.FileType.CSV.ordinal());
          PCollectionTuple jsonlRecords =
              inputFilesJsonl.apply("ProcessJSONL", ProcessJsonl.newBuilder().build());
          recordsList = recordsList.and(jsonlRecords.get(ProcessJsonl.recordsTuple));
          headerList = headerList.and(jsonlRecords.get(ProcessJsonl.headersTuple));
          break;

        case PARQUET:
          PCollection<KV<String, FileIO.ReadableFile>> inputFilesParquet =
              fileList.get(Util.FileType.CSV.ordinal());
          PCollectionTuple parquetRecords =
              inputFilesParquet.apply("ProcessPARQUET", ProcessParquet.newBuilder().build());
          recordsList = recordsList.and(parquetRecords.get(ProcessParquet.recordsTuple));
          headerList = headerList.and(parquetRecords.get(ProcessParquet.headersTuple));
          break;

        default:
          throw new IllegalArgumentException("Please validate FileType parameter");
      }
    }

    try {
      PCollection<KV<String, Table.Row>> flattenedRecords =
          recordsList
              .apply(Flatten.pCollections())
              .setCoder(
                  KvCoder.of(
                      StringUtf8Coder.of(),
                      input.getPipeline().getCoderRegistry().getCoder(Table.Row.class)));

      PCollection<KV<String, List<String>>> flattenedHeaders =
          headerList
              .apply("Merge Headers", Flatten.pCollections())
              .setCoder(KvCoder.of(StringUtf8Coder.of(), ListCoder.of(StringUtf8Coder.of())));

      return PCollectionTuple.of(tableRows, flattenedRecords).and(headersMap, flattenedHeaders);
    } catch (Exception e) {
      LOG.info("Found exception " + e.toString());
      throw new RuntimeException(e);
    }
  }
}
