/*-
 * -\-\-
 * Flo BigQuery
 * --
 * Copyright (C) 2016 - 2018 Spotify AB
 * --
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * -/-/-
 */

package com.spotify.flo.contrib.bigquery;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.cloud.ServiceOptions;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONTokener;

class GcpOptions {

  private static final String CREDENTIAL_ENV_NAME = "GOOGLE_APPLICATION_CREDENTIALS";
  private static final String LEGACY_PROJECT_ENV_NAME = "GCLOUD_PROJECT";
  private static final String PROJECT_ENV_NAME = "GOOGLE_CLOUD_PROJECT";

  /**
   * A version of {@link ServiceOptions#getDefaultProject()} that defaults to the service account
   * project instead of the GCE (metadata server) project id.
   */
  static String getDefaultProjectId() {
    String projectId = System.getProperty(PROJECT_ENV_NAME, System.getenv(PROJECT_ENV_NAME));
    if (projectId == null) {
      projectId = System.getProperty(LEGACY_PROJECT_ENV_NAME, System.getenv(LEGACY_PROJECT_ENV_NAME));
    }
    if (projectId == null) {
      projectId = getServiceAccountProjectId();
    }
    return (projectId != null) ? projectId : ServiceOptions.getDefaultProjectId();
  }

  /**
   * {@link ServiceOptions#getServiceAccountProjectId()} reimplemented here to avoid breaking if users use a version
   * of google-api-client that does not have {@link GoogleCredential#getServiceAccountProjectId()}.
   */
  private static String getServiceAccountProjectId() {
    String project = null;
    String credentialsPath = System.getenv(CREDENTIAL_ENV_NAME);
    if (credentialsPath != null) {
      try (InputStream credentialsStream = new FileInputStream(credentialsPath)) {
        JSONObject json = new JSONObject(new JSONTokener(credentialsStream));
        project = json.getString("project_id");
      } catch (IOException | JSONException ex) {
        // ignore
      }
    }
    return project;
  }
}
