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
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import java.io.IOException;

class BigQueryClientSingleton {

  private static final BigQuery BIGQUERY_INTERNAL;
  static final FloBigQueryClient BIGQUERY_CLIENT;

  static {
    final BigQueryOptions.Builder bigquery = BigQueryOptions.newBuilder();
    // Work around a bug in ServiceOptions.getDefaultProjectId() where it will use the project
    // returned by the GCE metadata server instead of the service account project
    // https://github.com/GoogleCloudPlatform/google-cloud-java/pull/2304/files#diff-966eb51fcb59c92eb46ebd5f532d8e52R404
    final String serviceAccountProjectId = getServiceAccountProjectId();
    if (serviceAccountProjectId != null) {
      bigquery.setProjectId(serviceAccountProjectId);
    }
    BIGQUERY_INTERNAL = bigquery.build().getService();
    BIGQUERY_CLIENT = new DefaultBigQueryClient(BIGQUERY_INTERNAL);
  }

  private static String getServiceAccountProjectId() {
    try {
      return GoogleCredential.getApplicationDefault().getServiceAccountProjectId();
    } catch (IOException e) {
      return null;
    }
  }
}
