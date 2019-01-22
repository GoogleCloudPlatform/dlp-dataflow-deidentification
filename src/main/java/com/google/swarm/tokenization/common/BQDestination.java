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

import java.util.Arrays;
import java.util.List;

import org.apache.beam.sdk.io.gcp.bigquery.DynamicDestinations;
import org.apache.beam.sdk.io.gcp.bigquery.TableDestination;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.ValueInSingleWindow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.services.bigquery.model.TableSchema;

@SuppressWarnings("serial")
public class BQDestination extends DynamicDestinations<Row, KV<String, List<String>>> {

	private static final Logger LOG = LoggerFactory.getLogger(BQDestination.class);

	private ValueProvider<String> datasetName;

	public BQDestination(ValueProvider<String> datasetName) {
		this.datasetName = datasetName;

	}

	@Override
	public KV<String, List<String>> getDestination(ValueInSingleWindow<Row> element) {

		String key = element.getValue().getTableId();
		String[] headers = element.getValue().getHeader();
		String table_name = String.format("%s.%s", this.datasetName, key);
		LOG.debug("Table Destination {}, {}", table_name, headers.length);
		return KV.of(table_name, Arrays.asList(headers));
	}

	@Override
	public TableDestination getTable(KV<String, List<String>> destination) {

		TableDestination dest = new TableDestination(destination.getKey(), "pii-tokenized output data from dataflow");
		LOG.debug("Table Destination {}", dest.toString());
		return dest;
	}

	@Override
	public TableSchema getSchema(KV<String, List<String>> destination) {

		TableSchema schema = Util.getSchema(destination.getValue());
		LOG.debug("***Schema {}", schema.toString());
		return schema;
	}

}
