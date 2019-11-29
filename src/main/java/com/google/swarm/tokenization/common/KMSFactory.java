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

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.cloudkms.v1.CloudKMS;
import com.google.api.services.cloudkms.v1.CloudKMSScopes;
import com.google.api.services.cloudkms.v1.model.DecryptRequest;
import com.google.api.services.cloudkms.v1.model.DecryptResponse;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.Collection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KMSFactory {

  public static final Logger LOG = LoggerFactory.getLogger(KMSFactory.class);
  private static CloudKMS instance = null;

  public static synchronized CloudKMS getService() throws IOException, GeneralSecurityException {
    if (instance == null) {
      instance = buildService();
    }
    return instance;
  }

  private static CloudKMS buildService() throws IOException, GeneralSecurityException {
    HttpTransport transport = GoogleNetHttpTransport.newTrustedTransport();
    JsonFactory jsonFactory = new JacksonFactory();
    GoogleCredential credential = GoogleCredential.getApplicationDefault(transport, jsonFactory);

    if (credential.createScopedRequired()) {
      Collection<String> scopes = CloudKMSScopes.all();
      credential = credential.createScoped(scopes);
    }

    return new CloudKMS.Builder(transport, jsonFactory, credential)
        .setApplicationName("Cloud KMS ")
        .build();
  }

  public static String decrypt(
      String projectId, String locationId, String keyRingId, String cryptoKeyId, String ciphertext)
      throws IOException, GeneralSecurityException {
    // Create the Cloud KMS client.
    if (projectId != null || keyRingId != null || cryptoKeyId != null || ciphertext != null) {
      CloudKMS kms = KMSFactory.getService();
      // The resource name of the cryptoKey
      String cryptoKeyName =
          String.format(
              "projects/%s/locations/%s/keyRings/%s/cryptoKeys/%s",
              projectId, locationId, keyRingId, cryptoKeyId);

      DecryptRequest request = new DecryptRequest().setCiphertext(ciphertext);
      DecryptResponse response =
          kms.projects()
              .locations()
              .keyRings()
              .cryptoKeys()
              .decrypt(cryptoKeyName, request)
              .execute();

      return response.getPlaintext().toString();
    }
    return null;
  }
}
