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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Serializable;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.FileIO.ReadableFile;
import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Watch;
import org.apache.beam.sdk.transforms.splittabledofn.OffsetRangeTracker;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.cloud.dlp.v2.DlpServiceClient;
import com.google.common.util.concurrent.RateLimiter;
import com.google.privacy.dlp.v2.ContentItem;
import com.google.privacy.dlp.v2.DeidentifyContentRequest;
import com.google.privacy.dlp.v2.DeidentifyContentResponse;
import com.google.privacy.dlp.v2.FieldId;
import com.google.privacy.dlp.v2.ProjectName;
import com.google.privacy.dlp.v2.Table;
import com.google.privacy.dlp.v2.Table.Row;
import com.google.swarm.tokenization.common.KMSFactory;
import com.google.swarm.tokenization.common.TokenizePipelineOptions;
import com.google.swarm.tokenization.common.Util;
import com.google.swarm.tokenization.common.WriteOneFilePerWindow;

public class CSVBatchPipeline {

	public static final Logger LOG = LoggerFactory
			.getLogger(CSVBatchPipeline.class);

	static RateLimiter rateLimiter = RateLimiter.create(10);
	static int batchSize =1; 
	static int rowCounter =0;
	static int totalNumberofRows =0;
	static List <String> rows = new ArrayList<>();
	static List<FieldId> headers = new ArrayList<>();
	
	@SuppressWarnings("serial")
	public static class FormatTableData
			extends
				DoFn<Table, KV<String, String>> {

		@ProcessElement
		public void processElement(ProcessContext c) {
			Table encryptedData = c.element();
			StringBuffer bufferedWriter = new StringBuffer();
			List<FieldId> outputHeaderFields = encryptedData.getHeadersList();
			List<Table.Row> outputRows = encryptedData.getRowsList();

			List<String> outputHeaders = outputHeaderFields.stream()
					.map(FieldId::getName).collect(Collectors.toList());

			bufferedWriter.append(String.join(",", outputHeaders) + "\n");

			String headerValue = bufferedWriter.toString().trim();

			bufferedWriter = new StringBuffer();
			for (Table.Row outputRow : outputRows) {
				String row = outputRow.getValuesList().stream()
						.map(value -> value.getStringValue())
						.collect(Collectors.joining(","));
				bufferedWriter.append(row + "\n");
			}

			String dataValues = bufferedWriter.toString().trim();
			// LOG.info("Format Data: " + bufferedWriter.toString());
			// c.output(bufferedWriter.toString().trim());

			c.output(KV.of(headerValue, dataValues));
		}
	}

	@SuppressWarnings("serial")

	public static class CreateDLPTable extends DoFn<String,Table> {
		
		void incrementCounter() {
			synchronized (this) {
				rowCounter = rowCounter+1;
			}
			
		}
		
		@ProcessElement
		public void processElement(ProcessContext c) throws IOException {
			
	
			rows.add(c.element());
			incrementCounter();
			rowCounter = rowCounter+1;
			//System.out.println("RowCounter: "+rowCounter+"TotalNumber of Rows: "+totalNumberofRows);
			if(batchSize==rows.size() || rowCounter==totalNumberofRows-1) {
				//System.out.println("Size: "+rows.size()+"Batch Size: "+batchSize);
				c.output(Util.createDLPTable(headers, rows));
				rows.clear();
				
				
			}
			
			
			
		}
		
	}
	
	@SuppressWarnings("serial")
	public static class CSVFileReader extends DoFn<ReadableFile, String>{
		private ValueProvider<Integer> batchSize;
		private ValueProvider<String> cSek;
		private ValueProvider<String> cSekhash;
		private ValueProvider<String> kmsKeyProjectName;
		private String objectName;
		private String bucketName;
		private String key;
		private boolean customerSuppliedKey;
		private ValueProvider<String> fileDecryptKey;
		private ValueProvider<String> fileDecryptKeyName;
		private BufferedReader br;
		public CSVFileReader(ValueProvider<String> kmsKeyProjectName,
				ValueProvider<String> fileDecryptKeyRing,
				ValueProvider<String> fileDecryptKey,
				ValueProvider<Integer> batchSize, ValueProvider<String> cSek,
				ValueProvider<String> cSekhash)
				throws IOException, GeneralSecurityException {

			this.batchSize = batchSize;
			this.kmsKeyProjectName = kmsKeyProjectName;
			this.fileDecryptKey = fileDecryptKey;
			this.fileDecryptKeyName = fileDecryptKeyRing;
			this.cSek = cSek;
			this.cSekhash = cSekhash;
			this.customerSuppliedKey = false;
			this.key = null;
			this.br = null;

		}

		@ProcessElement
		public void processElement(ProcessContext c, OffsetRangeTracker tracker)
				throws IOException, GeneralSecurityException {

			if (customerSuppliedKey)
				this.key = KMSFactory.decrypt(this.kmsKeyProjectName.get(),
						"global", this.fileDecryptKeyName.get(),
						this.fileDecryptKey.get(), this.cSek.get());

			this.bucketName = Util.parseBucketName(c.element().getMetadata()
					.resourceId().getCurrentDirectory().toString());

			this.objectName = c.element().getMetadata().resourceId()
					.getFilename().toString();

			
			// LOG.info("Process Element:" + " Bucket Name: " + bucketName
			// + " File Name: " + objectName + " CSK"
			// + this.customerSuppliedKey + " csek: " + this.cSek
			// + " csekhash: " + this.cSekhash + " key ring name: "
			// + this.fileDecryptKeyName + " Key: " + this.fileDecryptKey
			// + "Batch Size: " + this.batchSize+" BR: "+this.br);

				this.br = Util.getReader(this.customerSuppliedKey,
					this.objectName, this.bucketName, c.element(),
					this.key, this.cSekhash);
				headers = Arrays.stream(br.readLine().split(",")).map(
						header -> FieldId.newBuilder().setName(header).build())
						.collect(Collectors.toList());
			
				CSVBatchPipeline.batchSize=this.batchSize.get().intValue();
				
			   long startOffset= tracker.currentRestriction().getFrom();	
			
				for (long i=startOffset; tracker.tryClaim(i);i++) {
						this.br = Util.getReader(this.customerSuppliedKey,
								this.objectName, this.bucketName, c.element(),
								this.key, this.cSekhash);
						c.output(this.br.lines().skip(i - 1).findFirst().get());
						br.close();
				}
				
						
		}	
			 

		@GetInitialRestriction
		public OffsetRange getInitialRestriction(ReadableFile dataFile)
				throws IOException, GeneralSecurityException {

			// this.customerSuppliedKey = Util.findEncryptionType(
			// this.fileDecryptKeyName.get(), this.fileDecryptKey.get(),
			// this.cSek.get(), this.cSekhash.get());
			//
			//

			if (customerSuppliedKey)
				this.key = KMSFactory.decrypt(this.kmsKeyProjectName.get(),
						"global", this.fileDecryptKeyName.get(),
						this.fileDecryptKey.get(), this.cSek.get());

			this.bucketName = Util.parseBucketName(dataFile.getMetadata()
					.resourceId().getCurrentDirectory().toString());

			this.objectName = dataFile.getMetadata().resourceId().getFilename()
					.toString();

			this.br = Util.getReader(this.customerSuppliedKey, this.objectName,
					this.bucketName, dataFile, this.key, this.cSekhash);
			totalNumberofRows= Util.countRecords(br);
			OffsetRange range = new OffsetRange(2, totalNumberofRows + 1);
//			LOG.info("Initial Restriction Success: Range: " + range.getFrom()
//					+ " To: " + range.getTo());
			if (br != null)
				br.close();
			return range;

		}


		@SplitRestriction
		public void splitRestriction(ReadableFile element, OffsetRange range,
				OutputReceiver<OffsetRange> out) {

			
			
			for (final OffsetRange p : range.split(this.batchSize.get(),1)) {
				out.output(p);

			}
	
		
			
		}
		
		 @NewTracker
		 public OffsetRangeTracker newTracker(OffsetRange range) {
		

		 //LOG.info("New Tracker from: "+range.getFrom()+" To: "+range.getTo());
		 return new OffsetRangeTracker(new OffsetRange(range.getFrom(),
		 range.getTo()));
		
		 }

	
		

	}

	public static boolean checkRate() {

		return rateLimiter.tryAcquire(1, 10, TimeUnit.MILLISECONDS);
	}
	@SuppressWarnings("serial")
	public static class FormatOutputData
			extends
				DoFn<KV<String, Iterable<String>>, String> {
		@ProcessElement
		public void processElement(ProcessContext c) {

			KV<String, Iterable<String>> outputData = c.element();
			StringBuffer bufferedWriter = new StringBuffer();

			bufferedWriter.append(outputData.getKey() + "\n");

			outputData.getValue().forEach(value -> {

				bufferedWriter.append(value + "\n");

			});

			c.output(bufferedWriter.toString().trim());

		}
	}

	@SuppressWarnings("serial")
	public static class TokenizeData extends DoFn<Table, Table> {

		private ValueProvider<String> projectId;
		private ValueProvider<String> deIdentifyTemplateName;
		private ValueProvider<String> inspectTemplateName;
		private boolean inspectTemplateExist;
		public TokenizeData(ValueProvider<String> projectId,
				ValueProvider<String> deIdentifyTemplateName,
				ValueProvider<String> inspectTemplateName) {

			this.projectId = projectId;
			this.deIdentifyTemplateName = deIdentifyTemplateName;
			this.inspectTemplateName = inspectTemplateName;
			this.inspectTemplateExist = false;

		}

		@ProcessElement
		public void processElement(ProcessContext c) {

			// boolean proceed = rateLimiter.tryAcquire(1);

			if (true) {

				Table nonEncryptedData = c.element();
				Table encryptedData;
				// if (this.inspectTemplateName.get() != null)
				// this.inspectTemplateExist = true;

				try (DlpServiceClient dlpServiceClient = DlpServiceClient
						.create()) {

					ContentItem tableItem = ContentItem.newBuilder()
							.setTable(nonEncryptedData).build();
					DeidentifyContentRequest request;
					DeidentifyContentResponse response;

					if (this.inspectTemplateExist) {
						request = DeidentifyContentRequest.newBuilder()
								.setParent(ProjectName.of(this.projectId.get())
										.toString())
								.setDeidentifyTemplateName(
										this.deIdentifyTemplateName.get())
								.setInspectTemplateName(
										this.inspectTemplateName.get())
								.setItem(tableItem).build();
					} else {
						request = DeidentifyContentRequest.newBuilder()
								.setParent(ProjectName.of(this.projectId.get())
										.toString())
								.setDeidentifyTemplateName(
										this.deIdentifyTemplateName.get())
								.setItem(tableItem).build();

					}

					if (checkRate()) {
						response = dlpServiceClient.deidentifyContent(request);
						encryptedData = response.getItem().getTable();
						LOG.info("Request Size Successfully Tokenized: "
								+ request.toByteString().size() + " bytes"
								+ " Number of rows tokenized: "
								+ response.getItem().getTable().getRowsCount());
						c.output(encryptedData);
					} else {
						// LOG.info("Check Rate False.. Trying again");
					}

				} catch (IOException e) {
					e.printStackTrace();
				}
			}

		}

	}

	public static void main(String[] args)
			throws IOException, GeneralSecurityException {

		TokenizePipelineOptions options = PipelineOptionsFactory.fromArgs(args)
				.withValidation().as(TokenizePipelineOptions.class);

		Pipeline p = Pipeline.create(options);
		p.apply(FileIO.match().filepattern(options.getInputFile())
				// 10 seconds polling
				.continuously(Duration.standardSeconds(60),
						Watch.Growth.never()))
				.apply(FileIO.readMatches()
						.withCompression(Compression.UNCOMPRESSED))
				.apply("CSV File Reader",
						ParDo.of(new CSVFileReader(options.getDlpProject(),
								options.getFileDecryptKeyName(),
								options.getFileDecryptKey(),
								options.getBatchSize(), options.getCsek(),
								options.getCsekhash())))
				.apply("Create Table",ParDo.of(new CreateDLPTable()))
				.apply("Tokenize Data",
						ParDo.of(new TokenizeData(options.getDlpProject(),
								options.getDeidentifyTemplateName(),
								options.getInspectTemplateName())))
				.apply("Format Table Data", ParDo.of(new FormatTableData()))

				// 1 minute window
				.apply(Window.<KV<String, String>>into(
						FixedWindows.of(Duration.standardMinutes(1))))

				.apply(GroupByKey.<String, String>create())
				.apply("Format Output Data", ParDo.of(new FormatOutputData()))
				.apply(new WriteOneFilePerWindow(options.getOutputFile(), 1));

		p.run();
	}

}