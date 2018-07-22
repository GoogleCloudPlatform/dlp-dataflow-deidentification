
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
import java.io.InputStream;
import java.security.GeneralSecurityException;
import java.util.Collection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.http.HttpHeaders;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.storage.Storage;
import com.google.api.services.storage.StorageScopes;

public class StorageFactory {

	public static final Logger LOG = LoggerFactory
			.getLogger(StorageFactory.class);
	private static Storage instance = null;

	public static synchronized Storage getService()
			throws IOException, GeneralSecurityException {
		if (instance == null) {
			instance = buildService();
		}
		return instance;
	}

	private static Storage buildService()
			throws IOException, GeneralSecurityException {
		HttpTransport transport = GoogleNetHttpTransport.newTrustedTransport();
		JsonFactory jsonFactory = new JacksonFactory();
		GoogleCredential credential = GoogleCredential
				.getApplicationDefault(transport, jsonFactory);

		if (credential.createScopedRequired()) {
			Collection<String> scopes = StorageScopes.all();
			credential = credential.createScoped(scopes);
		}

		return new Storage.Builder(transport, jsonFactory, credential)
				.setApplicationName("GCS").build();
	}
	public static InputStream downloadObject(Storage storage, String bucketName,
			String objectName, String base64CseKey, String base64CseKeyHash)
			throws Exception {

		// Set the CSEK headers
		final HttpHeaders httpHeaders = new HttpHeaders();
		httpHeaders.set("x-goog-encryption-algorithm", "AES256");
		httpHeaders.set("x-goog-encryption-key", base64CseKey);
		httpHeaders.set("x-goog-encryption-key-sha256", base64CseKeyHash);
		Storage.Objects.Get getObject = storage.objects().get(bucketName,
				objectName);
		getObject.setRequestHeaders(httpHeaders);

		try {

			return getObject.executeMediaAsInputStream();
		} catch (GoogleJsonResponseException e) {
			LOG.info("Error downloading: " + e.getContent());
			System.exit(1);
			return null;
		}
	}

}