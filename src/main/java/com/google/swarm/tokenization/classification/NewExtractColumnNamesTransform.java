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
package com.google.swarm.tokenization.classification;

import com.google.auto.value.AutoValue;
import com.google.swarm.tokenization.avro.AvroColumnNamesDoFn;
import com.google.swarm.tokenization.common.CSVColumnNamesDoFn;
import com.google.swarm.tokenization.common.ResolveDuplicatesCombineFn;
import com.google.swarm.tokenization.common.Util.FileType;
import com.google.swarm.tokenization.json.JsonColumnNameDoFn;
import com.google.swarm.tokenization.orc.ORCColumnNameDoFn;
import com.google.swarm.tokenization.parquet.ParquetColumnNamesDoFn;
import com.google.swarm.tokenization.txt.TxtColumnNameDoFn;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionView;

@SuppressWarnings("serial")
@AutoValue
public abstract class NewExtractColumnNamesTransform
        extends PTransform<
        PCollectionList<KV<String, FileIO.ReadableFile>>,
        PCollectionView<Map<String, List<String>>>> {

    @Nullable
    public abstract List<String> headers();

    public abstract Character columnDelimiter();

    public abstract Boolean pubSubGcs();

    public abstract String projectId();

    public abstract List<FileType> fileTypes();

    @AutoValue.Builder
    public abstract static class Builder {

        public abstract NewExtractColumnNamesTransform.Builder setHeaders(List<String> headers);

        public abstract NewExtractColumnNamesTransform.Builder setColumnDelimiter(
                Character columnDelimiter);

        public abstract NewExtractColumnNamesTransform.Builder setPubSubGcs(Boolean pubSubGcs);

        public abstract NewExtractColumnNamesTransform.Builder setProjectId(String projectId);

        public abstract NewExtractColumnNamesTransform.Builder setFileTypes(List<FileType> fileTypes);

        public abstract NewExtractColumnNamesTransform build();
    }

    public static NewExtractColumnNamesTransform.Builder newBuilder() {
        return new AutoValue_NewExtractColumnNamesTransform.Builder();
    }

    @Override
    public PCollectionView<Map<String, List<String>>> expand(
            PCollectionList<KV<String, FileIO.ReadableFile>> input) {
        PCollectionList<KV<String, List<String>>> headerList = PCollectionList.empty(input.getPipeline());

        for (FileType fileType : fileTypes()) {
            switch (fileType) {
                case AVRO:
                    PCollection<KV<String, List<String>>> readHeaderAvro =
                            input
                                    .get(fileType.ordinal())
                                    .apply("ReadHeaderAVRO", ParDo.of(new AvroColumnNamesDoFn()));
                    headerList = headerList.and(readHeaderAvro);
                    break;

                case CSV:
                    PCollection<KV<String, List<String>>> readHeaderCsv =
                            input
                                    .get(fileType.ordinal())
                                    .apply("ReadHeaderCSV", ParDo.of(new CSVColumnNamesDoFn(columnDelimiter())));
                    headerList = headerList.and(readHeaderCsv);
                    break;

                case JSONL:
                    PCollection<KV<String, List<String>>> readHeaderJsonl =
                            input
                                    .get(fileType.ordinal())
                                    .apply("ReadHeaderJSONL", ParDo.of(new JsonColumnNameDoFn(headers())));
                    headerList = headerList.and(readHeaderJsonl);
                    break;

                case ORC:
                    PCollection<KV<String, List<String>>> readHeaderOrc =
                            input
                                    .get(fileType.ordinal())
                                    .apply("ReadHeaderORC", ParDo.of(new ORCColumnNameDoFn(projectId())));
                    headerList = headerList.and(readHeaderOrc);
                    break;

                case PARQUET:
                    PCollection<KV<String, List<String>>> readHeaderParquet =
                            input
                                    .get(fileType.ordinal())
                                    .apply("ReadHeaderPARQUET", ParDo.of(new ParquetColumnNamesDoFn()));
                    headerList = headerList.and(readHeaderParquet);
                    break;

                case TSV:
                    PCollection<KV<String, List<String>>> readHeaderTsv =
                            input
                                    .get(fileType.ordinal())
                                    .apply("ReadHeaderTSV", ParDo.of(new CSVColumnNamesDoFn('\t')));
                    headerList = headerList.and(readHeaderTsv);
                    break;

                case TXT:
                    PCollection<KV<String, List<String>>> readHeaderTxt =
                            input
                                    .get(fileType.ordinal())
                                    .apply("ReadHeaderTXT", ParDo.of(new TxtColumnNameDoFn(headers())));
                    headerList = headerList.and(readHeaderTxt);
                    break;

                default:
                    throw new IllegalStateException("Unexpected value: " + fileType);
            }
        }

//        readHeader = headerList.apply("Merge headers",Flatten.pCollections());
//
//        if (!pubSubGcs()) {
//            return readHeader.apply("ViewAsList", View.asMap());
//        }
//
//        return readHeader
//                .apply(Combine.<String, List<String>, List<String>>perKey(new ResolveDuplicatesCombineFn()))
//                .apply("ViewAsList", View.asMap());

        return headerList.apply("Merge Headers",Flatten.pCollections()).setCoder(KvCoder.of(StringUtf8Coder.of(), ListCoder.of(StringUtf8Coder.of())))
                .apply("ViewAsList", View.asMap());
    }
}
