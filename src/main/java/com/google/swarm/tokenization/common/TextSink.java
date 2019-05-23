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

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;

import org.apache.beam.sdk.io.FileIO.Sink;
import org.apache.beam.sdk.values.KV;

@SuppressWarnings("serial")

public class TextSink implements Sink<KV<String, String>> {
	private PrintWriter writer;

	@Override
	public void open(WritableByteChannel channel) throws IOException {
		writer = new PrintWriter(Channels.newOutputStream(channel));

	}

	@Override
	public void write(KV<String, String> element) {
		// Iterator<String> valueIterator = element.getValue().iterator();
		// StringBuilder contents = new StringBuilder();
//		while (valueIterator.hasNext()) {
//			contents.append(valueIterator.next());
//
//		}
		writer.println(element.getValue());
	}

	@Override
	public void flush() throws IOException {
		writer.flush();
	}
}