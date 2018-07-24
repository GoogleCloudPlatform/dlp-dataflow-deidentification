package com.google.swarm.tokenization.common;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.privacy.dlp.v2.FieldId;
import com.google.privacy.dlp.v2.Table;
import com.google.privacy.dlp.v2.Value;
import com.google.swarm.tokenization.CSVBatchPipeline;

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
		if (keyRing != null || keyName != null || csek != null
				|| csekhash != null)
			return true;
		else
			return false;
	}

}
