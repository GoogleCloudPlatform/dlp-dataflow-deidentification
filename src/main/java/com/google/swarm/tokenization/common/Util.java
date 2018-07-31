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

import com.google.privacy.dlp.v2.FieldId;
import com.google.privacy.dlp.v2.Table;
import com.google.privacy.dlp.v2.Value;
import java.io.BufferedReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Util {

	public static final Logger LOG = LoggerFactory.getLogger(Util.class);
	public static String parseBucketName(String value) {
		// gs://name/ -> name
		return value.substring(5, value.length() - 1);
	}
	private static Table.Row convertCsvRowToTableRow(String row) {
		String[] values = row.split(",");
		Table.Row.Builder tableRowBuilder = Table.Row.newBuilder();
		for (String value : values) {
			tableRowBuilder.addValues(
					Value.newBuilder().setStringValue(value).build());
		}

		return tableRowBuilder.build();
	}
	public static List<String> readBatch(BufferedReader reader,
			Integer batchSize) throws IOException {
		List<String> result = new ArrayList<>();

		for (int i = 0; i < batchSize.intValue(); i++) {
			String line = reader.readLine();
			if (line != null) {
				result.add(line);
			} else {
				return result;
			}
		}
		return result;
	}
	public static Table createDLPTable(List<FieldId> headers,
			List<String> lines) {

		List<Table.Row> rows = new ArrayList<>();
		lines.forEach(line -> {
			rows.add(convertCsvRowToTableRow(line));
		});
		Table table = Table.newBuilder().addAllHeaders(headers).addAllRows(rows)
				.build();

		return table;

	}
	public static boolean findEncryptionType(String keyRing, String keyName,
			String csek, String csekhash) {

		LOG.info("findEncryptionType:" + keyRing + " " + keyName + " " + csek
				+ " " + csekhash);
		return keyRing != null || keyName != null || csek != null
				|| csekhash != null;
	}

}
