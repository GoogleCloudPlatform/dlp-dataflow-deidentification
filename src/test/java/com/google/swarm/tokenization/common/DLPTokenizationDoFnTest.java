package com.google.swarm.tokenization.common;

import com.google.privacy.dlp.v2.ContentItem;
import com.google.privacy.dlp.v2.DeidentifyContentRequest;
import com.google.privacy.dlp.v2.Table;
import com.google.privacy.dlp.v2.Value;
import org.apache.beam.sdk.options.ValueProvider;
import org.junit.Test;

import static org.junit.Assert.*;

public class DLPTokenizationDoFnTest {
	@Test
	public void testSetInspectTemplateExist() {
		DLPTokenizationDoFn dlp = new DLPTokenizationDoFn("Project Name",
				ValueProvider.StaticValueProvider.of("DeidentifyTemplateName"),
				ValueProvider.StaticValueProvider.of("IdentifyTemplateName"));

		assertEquals(dlp.getInspectTemplateExist(), false);
		dlp.setInspectTemplateExist();
		assertEquals(dlp.getInspectTemplateExist(), true);

	}

	@Test
	public void testBuildDeidentifyContentRequest() {
		DLPTokenizationDoFn dlp = new DLPTokenizationDoFn("Project Name",
				ValueProvider.StaticValueProvider.of("DeidentifyTemplateName"),
				ValueProvider.StaticValueProvider.of("IdentifyTemplateName"));

		ContentItem contentItem = ContentItem.newBuilder().build();

		dlp.setInspectTemplateExist();
		DeidentifyContentRequest request = dlp.buildDeidentifyContentRequest(contentItem);

		assertEquals(request.getParent(), "projects/Project Name");
		assertEquals(request.getDeidentifyTemplateName(), "DeidentifyTemplateName");
		assertEquals(request.getInspectTemplateName(), "IdentifyTemplateName");

	}

	@Test
	public void testConvertTableRowToRow() {
		DLPTokenizationDoFn dlp = new DLPTokenizationDoFn("Project Name",
				ValueProvider.StaticValueProvider.of("DeidentifyTemplateName"),
				ValueProvider.StaticValueProvider.of("IdentifyTemplateName"));

		String[] header = { "header0", "header1" };
		String key = "Key name";
		Table.Row.Builder tableRowBuilder = Table.Row.newBuilder();
		tableRowBuilder.addValues(0, Value.newBuilder().setStringValue("value0"));
		tableRowBuilder.addValues(1, Value.newBuilder().setStringValue("value1"));
		Table.Row row = tableRowBuilder.build();

		Row result = dlp.convertTableRowToRow(header, key, row);
		assertEquals(result.getTableId(), key);
		assertEquals(result.getHeader()[0], "header0");
		assertEquals(result.getValue()[1], "value1");

	}

}
