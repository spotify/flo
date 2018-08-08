package com.spotify.flo.contrib.bigquery;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.cloud.ServiceOptions;
import java.io.IOException;

class GcpOptions {

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

  private static String getServiceAccountProjectId() {
    try {
      return GoogleCredential.getApplicationDefault().getServiceAccountProjectId();
    } catch (IOException e) {
      return null;
    }
  }
}
