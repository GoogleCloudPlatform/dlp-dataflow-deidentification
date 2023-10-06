/*
 * Copyright 2020 Google LLC
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
package com.google.swarm.tokenization.tool;

import static java.nio.charset.StandardCharsets.UTF_8;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.common.io.Resources;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;
import java.net.URI;
import org.apache.beam.sdk.extensions.gcp.util.gcsfs.GcsPath;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DLPTemplateHelper {
  public static final Logger LOG = LoggerFactory.getLogger(DLPTemplateHelper.class);
  public static final String DLP_DEID_CONFIG_FILE = "de-identify-config.config";
  public static final String DLP_REID_CONFIG_FILE = "re-identify-config.config";
  public static final String DLP_INSPECT_CONFIG_FILE = "inspect-config.config";

  public static final Gson gson = new Gson();

  public static void main(String[] args) {
    long timeStamp = Instant.now().getMillis();
    String deidDlpConfig = getDLPConfigJson(DLP_DEID_CONFIG_FILE);
    String reidDlpConfig = getDLPConfigJson(DLP_REID_CONFIG_FILE);
    String gcsBucket = args[0];
    JsonObject kekConfig =
        gson.fromJson(getKekDetails(gcsBucket), new TypeToken<JsonObject>() {}.getType());
    JsonElement kek = kekConfig.get("ciphertext");
    // take out version -DLP template does not allow
    String keyName =
        gson.toJson((kekConfig.get("name").getAsString().split("/cryptoKeyVersions/")[0]));

    String updatedDeidConfig = String.format(deidDlpConfig, kek, keyName, kek, keyName, timeStamp);
    LOG.info(
        "*****Successfully Updated DLP De-Identification Configuration With KEK {} *****",
        uploadConfig(updatedDeidConfig, gcsBucket, "de-identify-config.json"));

    String updatedReidConfig = String.format(reidDlpConfig, kek, keyName, timeStamp);
    LOG.info(
        "*****Successfully Updated DLP Re-Identification Configuration With KEK {} *****",
        uploadConfig(updatedReidConfig, gcsBucket, "re-identify-config.json"));

    LOG.info(
        "*****Successfully Uploaded DLP Inspect Configuration With Info Types {} *****",
        uploadConfig(
            String.format(getDLPConfigJson(DLP_INSPECT_CONFIG_FILE), timeStamp),
            gcsBucket,
            "inspect-config.json"));
  }

  public static String getKekDetails(String gcsPath) {
    GcsPath path = GcsPath.fromUri(URI.create(gcsPath));
    Storage storage = StorageOptions.getDefaultInstance().getService();
    BlobId blobId = BlobId.of(path.getBucket(), path.getObject());
    byte[] content = storage.readAllBytes(blobId);
    String contentString = new String(content, UTF_8);
    return contentString;
  }

  public static BlobId uploadConfig(String contents, String gcsPath, String fileName) {
    GcsPath path = GcsPath.fromUri(URI.create(gcsPath));
    Storage storage = StorageOptions.getDefaultInstance().getService();
    BlobId blobId = BlobId.of(path.getBucket(), fileName);
    BlobInfo blobInfo = BlobInfo.newBuilder(blobId).setContentType("application/json").build();
    Blob blob = storage.create(blobInfo, contents.getBytes(UTF_8));
    return blob.getBlobId();
  }

  public static String getDLPConfigJson(String filePath) {
    String schemaJson = null;
    try {
      schemaJson = Resources.toString(Resources.getResource(filePath), UTF_8);
    } catch (Exception e) {
      LOG.error("Unable to read {} file from the resources folder!", DLP_DEID_CONFIG_FILE, e);
    }

    return schemaJson;
  }

  public int adder(int a, int b) {
    return a + b;
  }
}
