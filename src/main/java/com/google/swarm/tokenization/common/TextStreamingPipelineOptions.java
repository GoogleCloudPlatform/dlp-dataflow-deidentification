
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

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.ValueProvider;

public interface TextStreamingPipelineOptions extends PipelineOptions {
	@Description("Project to use for DLP calls")
	ValueProvider<String> getDlpProject();
	void setDlpProject(ValueProvider<String> project);

	@Description("Path of the file to read from")
	ValueProvider<String> getInputFile();
	void setInputFile(ValueProvider<String> value);

	@Description("Path of the file to write to")
	ValueProvider<String> getOutputFile();
	void setOutputFile(ValueProvider<String> value);

	@Description("Template to DeIdentiy")
	ValueProvider<String> getDeidentifyTemplateName();
	void setDeidentifyTemplateName(ValueProvider<String> value);

	@Description("Template to Inspect")
	ValueProvider<String> getInspectTemplateName();
	void setInspectTemplateName(ValueProvider<String> value);

	@Description("Default batch Size (bytes)")
	@Default.Integer(1024)
	ValueProvider<Integer> getBatchSize();
	void setBatchSize(ValueProvider<Integer> value);

	@Description("encryption_key (CSEK) to decrypt the bucket")
	@Default.String("0xh0XFLhgE5WiD73TuARo71jSOl8FONcqin4+00jSLg=")
	ValueProvider<String> getCsek();
	void setCsek(ValueProvider<String> value);

	@Description("CSEK hash to decrypt the bucket")
	@Default.String("lzjD1iV85ZqaF/C+uGrVWsLq2bdN7nGIruTjT/mgNIE=")
	ValueProvider<String> getCsekhash();
	void setCsekhash(ValueProvider<String> value);

	@Description("GCS File Decryption Key Name")
	@Default.String("gcs-bucket-encryption")
	ValueProvider<String> getFileDecryptKey();
	void setFileDecryptKey(ValueProvider<String> value);

	@Description("GCS File Decryption Key Ring Name")
	@Default.String("data-file-key")
	ValueProvider<String> getFileDecryptKeyName();
	void setFileDecryptKeyName(ValueProvider<String> value);
}
