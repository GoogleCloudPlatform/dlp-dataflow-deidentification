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
import java.util.Map;

import org.apache.beam.sdk.io.gcp.bigquery.DynamicDestinations;
import org.apache.beam.sdk.io.gcp.bigquery.TableDestination;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.ValueInSingleWindow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.services.bigquery.model.TableCell;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;

public class BQDestination extends DynamicDestinations<KV<String, TableRow>, KV<String, TableRow>> {

	private static final Logger LOG = LoggerFactory.getLogger(BQDestination.class);

	private static final long serialVersionUID = 1L;
	private ValueProvider<String> datasetName;
	private String projectId;

	public BQDestination(ValueProvider<String> datasetName, String projectId) {
		this.datasetName = datasetName;
		this.projectId = projectId;

	}

	@Override
	public KV<String, TableRow> getDestination(ValueInSingleWindow<KV<String, TableRow>> element) {
		String key = element.getValue().getKey();
		String tableName = String.format("%s:%s.%s", projectId, datasetName.get(), key);
		LOG.debug("Table Name {}", tableName);
		return KV.of(tableName, element.getValue().getValue());
	}

	@Override
	public TableDestination getTable(KV<String, TableRow> destination) {
		TableDestination dest = new TableDestination(destination.getKey(), "pii-tokenized output data from dataflow");
		LOG.debug("Table Destination {}", dest.getTableSpec());
		return dest;
	}

	@Override
	public TableSchema getSchema(KV<String, TableRow> destination) {

		TableRow bqRow = destination.getValue();
		TableSchema schema = new TableSchema();
		List<TableFieldSchema> fields = new ArrayList<TableFieldSchema>();
		// When TableRow is created in earlier steps, setF() was
		// used to setup TableCells so that Table Schema can be constructed

		List<TableCell> cells = bqRow.getF();
		for (int i = 0; i < cells.size(); i++) {

			Map<String, Object> object = cells.get(i);
			String header = object.keySet().iterator().next();
			String type = Util.typeCheck(object.get(header).toString());
			LOG.debug("Type {}, header {}, value {}", type, header, object.get(header).toString());
			if (type.equals("RECORD")) {
				String keyValuePair = object.get(header).toString();
				String[] records = keyValuePair.split(",");
				List<TableFieldSchema> nestedFields = new ArrayList<TableFieldSchema>();

				for (int j = 0; j < records.length; j++) {
					String[] element = records[j].substring(1).split("=");
					String elementValue = element[1].substring(0, element[1].length() - 1);
					String elementType = Util.typeCheck(elementValue.trim());
					LOG.debug("element header {} , element type {}, element Value {}", element[0], elementType,
							elementValue);
					nestedFields.add(new TableFieldSchema().setName(element[0]).setType(elementType));
				}
				fields.add(new TableFieldSchema().setName(header).setType(type).setFields(nestedFields));

			} else {
				fields.add(new TableFieldSchema().setName(header).setType(type));

			}

		}
		schema.setFields(fields);
		return schema;
	}

}