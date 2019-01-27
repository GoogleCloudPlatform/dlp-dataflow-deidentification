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
package com.google.swarm.tokenization;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.List;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.Watch;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.privacy.dlp.v2.Table;
import com.google.swarm.tokenization.common.BQDestination;
import com.google.swarm.tokenization.common.BQTableRowSF;
import com.google.swarm.tokenization.common.CSVContentProcessorDoFn;
import com.google.swarm.tokenization.common.CSVReader;
import com.google.swarm.tokenization.common.CSVSink;
import com.google.swarm.tokenization.common.DLPTokenizationDoFn;
import com.google.swarm.tokenization.common.Row;
import com.google.swarm.tokenization.common.TokenizePipelineOptions;

public class CSVStreamingPipeline {

	public static final Logger LOG = LoggerFactory.getLogger(CSVStreamingPipeline.class);

	@SuppressWarnings("serial")
	public static void doTokenization(TokenizePipelineOptions options) {
		Pipeline p = Pipeline.create(options);

		PCollection<KV<String, List<String>>> filesAndContents = p
				.apply(FileIO.match().filepattern(options.getInputFile())
						.continuously(Duration.standardSeconds(options.getPollingInterval()), Watch.Growth.never()))
				.apply(FileIO.readMatches().withCompression(Compression.UNCOMPRESSED)).apply("FileHandler",
						ParDo.of(new CSVReader(options.getCsek(), options.getCsekhash(),
								options.getFileDecryptKeyName(), options.getFileDecryptKey(),
								options.as(GcpOptions.class).getProject(), options.getBatchSize())));

		PCollection<KV<String, Table>> dlpTables = filesAndContents.apply("ContentHandler",
				ParDo.of(new CSVContentProcessorDoFn(options.getBatchSize())));

		PCollection<Row> dlpRows = dlpTables
				.apply("DoDLPTokenization",
						ParDo.of(new DLPTokenizationDoFn(options.as(GcpOptions.class).getProject(),
								options.getDeidentifyTemplateName(), options.getInspectTemplateName())))
				.apply(Window.<Row>into(FixedWindows.of(Duration.standardSeconds(options.getInterval())))
						.triggering(
								AfterProcessingTime.pastFirstElementInPane().plusDelayOf(Duration.standardMinutes(1)))
						.discardingFiredPanes().withAllowedLateness(Duration.standardMinutes(1)));

		dlpRows.apply("WriteToBQ",
				BigQueryIO.<Row>write().to(new BQDestination(options.getDataset(),
						options.as(GcpOptions.class).getProject()))
						.withFormatFunction(new BQTableRowSF())
						.withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
						.withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED));

		dlpRows.apply(MapElements.via(new SimpleFunction<Row, KV<String, Row>>() {
			@Override
			public KV<String, Row> apply(Row row) {
				return KV.of(row.getTableId(), row);
			}
		})).apply(GroupByKey.<String, Row>create()).apply("WriteToGCS",
				FileIO.<String, KV<String, Iterable<Row>>>writeDynamic()
						.by((SerializableFunction<KV<String, Iterable<Row>>, String>) row -> {
							return row.getKey();
						}).via(new CSVSink()).to(options.getOutputFile()).withDestinationCoder(StringUtf8Coder.of())
						.withNumShards(1).withNaming(key -> FileIO.Write.defaultNaming(key, ".csv")));

		p.run();
	}

	public static void main(String[] args) throws IOException, GeneralSecurityException {

		TokenizePipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
				.as(TokenizePipelineOptions.class);

		doTokenization(options);

	}
}