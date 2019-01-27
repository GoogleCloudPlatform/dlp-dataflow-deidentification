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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.List;

import org.apache.beam.sdk.io.FileIO.ReadableFile;
import org.apache.beam.sdk.options.ValueProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.client.util.Charsets;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.api.services.storage.Storage;
import com.google.privacy.dlp.v2.FieldId;
import com.google.privacy.dlp.v2.Table;
import com.google.privacy.dlp.v2.Value;

public class Util {

	public static final Logger LOG = LoggerFactory.getLogger(Util.class);

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
		lines.forEach(line -> {
			rows.add(convertCsvRowToTableRow(line));
		});
		Table table = Table.newBuilder().addAllHeaders(headers).addAllRows(rows).build();

		return table;

	}

	public static boolean findEncryptionType(String keyRing, String keyName, String csek, String csekhash) {

		return keyRing != null || keyName != null || csek != null || csekhash != null;
	}

	public static BufferedReader getReader(boolean customerSuppliedKey, String objectName, String bucketName,
			ReadableFile file, String key, ValueProvider<String> csekhash) {

		BufferedReader br = null;

		try {
			if (!customerSuppliedKey) {
				ReadableByteChannel channel = file.openSeekable();
				// Charsets.ISO_8859_1.name()
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
					objectData = StorageFactory.downloadObject(storage, bucketName, objectName, key, csekhash.get());
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
		return checkedHeader;
	}
	@SuppressWarnings("serial")
	public static TableSchema getSchema(List<String> outputHeaders) {
		return new TableSchema().setFields(new ArrayList<TableFieldSchema>() {

			{

				outputHeaders.forEach(header -> {

					add(new TableFieldSchema().setName(checkHeaderName(header.trim())).setType("STRING"));

				});

			}

		});

	}
}
