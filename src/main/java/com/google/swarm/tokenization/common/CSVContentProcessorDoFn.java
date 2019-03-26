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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.splittabledofn.OffsetRangeTracker;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.privacy.dlp.v2.FieldId;
import com.google.privacy.dlp.v2.Table;
import com.google.swarm.tokenization.CSVStreamingPipeline;

@SuppressWarnings("serial")
public class CSVContentProcessorDoFn extends DoFn<KV<String, List<String>>, KV<String, Table>> {
	public static final Logger LOG = LoggerFactory.getLogger(CSVStreamingPipeline.class);

	private int numberOfRows;
	private ValueProvider<Integer> batchSize;

	public CSVContentProcessorDoFn(ValueProvider<Integer> batchSize) {

		this.batchSize = batchSize;
	}

	public KV<Integer, Integer> createStartEnd(int rowSize, long i) {
		int startOfLine;
		int endOfLine = (int) (i * this.batchSize.get().intValue());
		if (endOfLine > rowSize) {
			endOfLine = rowSize;
			startOfLine = (int) (((i - 1) * this.batchSize.get().intValue()) + 1);
		} else {
			startOfLine = (endOfLine - this.batchSize.get()) + 1;
		}

		return KV.of(startOfLine, endOfLine);
	}

	@ProcessElement
	public void processElement(ProcessContext c, OffsetRangeTracker tracker) {
		for (long i = tracker.currentRestriction().getFrom(); tracker.tryClaim(i); ++i) {
			String fileName = c.element().getKey();
			String key = String.format("%s_%d", fileName, i);
			List<String> rows = c.element().getValue().stream().skip(1).collect(Collectors.toList());
			List<FieldId> headers = Arrays.stream(c.element().getValue().get(0).split(","))
					.map(header -> FieldId.newBuilder().setName(header).build()).collect(Collectors.toList());
			KV<Integer, Integer> lineRange = createStartEnd(rows.size(), i);
			int startOfLine = lineRange.getKey();
			int endOfLine = lineRange.getValue();

			List<String> lines = new ArrayList<>();

			for (int index = startOfLine - 1; index < endOfLine; index++) {

				lines.add(rows.get(index));
			}
			Table batchData = Util.createDLPTable(headers, lines);

			if (batchData.getRowsCount() > 0) {
				LOG.info(
						"Current Restriction From: {}, Current Restriction To: {}, StartofLine: {}, End Of Line {}, BatchData {}",
						tracker.currentRestriction().getFrom(), tracker.currentRestriction().getTo(), startOfLine,
						endOfLine, batchData.getRowsCount());
				c.output(KV.of(key, batchData));
				lines.clear();
			}

		}

	}

	@GetInitialRestriction
	public OffsetRange getInitialRestriction(KV<String, List<String>> contents) {

		this.numberOfRows = contents.getValue().size() - 1;
		int totalSplit = 0;
		totalSplit = this.numberOfRows / this.batchSize.get().intValue();
		int remaining = this.numberOfRows % this.batchSize.get().intValue();
		if (remaining > 0) {
			totalSplit = totalSplit + 2;

		} else {
			totalSplit = totalSplit + 1;
		}
		LOG.info("Initial Restriction range from 1 to: {}", totalSplit);
		return new OffsetRange(1, totalSplit);

	}

	@SplitRestriction
	public void splitRestriction(KV<String, List<String>> contents, OffsetRange range,
			OutputReceiver<OffsetRange> out) {
		for (final OffsetRange p : range.split(1, 1)) {
			out.output(p);

		}
	}

	@NewTracker
	public OffsetRangeTracker newTracker(OffsetRange range) {
		return new OffsetRangeTracker(new OffsetRange(range.getFrom(), range.getTo()));

	}

}