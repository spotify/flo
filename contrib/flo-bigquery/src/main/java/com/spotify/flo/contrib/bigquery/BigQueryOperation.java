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

import static com.google.cloud.bigquery.QueryJobConfiguration.newBuilder;

import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.spotify.flo.Fn;
import com.spotify.flo.TaskBuilder.F1;
import java.io.Serializable;
import java.util.Objects;
import java.util.UUID;

/**
 * A BigQuery operation to be executed by the {@link BigQueryOperator}.
 */
public class BigQueryOperation<T> implements Serializable {

  private static final long serialVersionUID = 1L;

  Fn<JobInfo> jobRequest;
  F1<JobInfo, T> success;

  /**
   * Run a job. This can be a query, copy, load or extract with results written to a table, etc.
   */
  BigQueryOperation<T> job(Fn<JobInfo> jobRequest) {
    this.jobRequest = Objects.requireNonNull(jobRequest);
    return this;
  }

  /**
   * Specify some action to take on success. E.g. publishing a staging table.
   */
  BigQueryOperation<T> success(F1<JobInfo, T> success) {
    this.success = Objects.requireNonNull(success);
    return this;
  }

  static <T> BigQueryOperation<T> ofJob(Fn<JobInfo> job) {
    return new BigQueryOperation<T>().job(job);
  }

  static <T> BigQueryOperation<T> ofJob(Fn<JobInfo> job, F1<JobInfo, T> success) {
    return new BigQueryOperation<T>().job(job).success(success);
  }

  public static class Provider<T> implements Serializable {

    private static final long serialVersionUID = 1L;

    Provider() {
    }

    public BigQueryOperation<T> bq() {
      return new BigQueryOperation<>();
    }

    /**
     * Use standard SQL syntax for queries.
     * See: https://cloud.google.com/bigquery/sql-reference/
     *
     * @param query the standard (non legacy) SQL statement
     *
     * @return a {@link BigQueryOperation} instance to be executed by the {@link BigQueryOperator}
     */
    public BigQueryOperation<T> query(String query) {
      final QueryJobConfiguration queryConfig = newBuilder(query)
          .setUseLegacySql(false)
          .build();
      final JobId jobId = JobId.of(UUID.randomUUID().toString());
      return job(JobInfo.newBuilder(queryConfig).setJobId(jobId).build());
    }

    public BigQueryOperation<T> job(Fn<JobInfo> jobInfo) {
      return BigQueryOperation.ofJob(jobInfo);
    }

    public BigQueryOperation<T> job(JobInfo jobInfo) {
      return BigQueryOperation.ofJob(() -> jobInfo);
    }

    public BigQueryOperation<T> job(JobInfo jobInfo, F1<JobInfo, T> success) {
      return BigQueryOperation.ofJob(() -> jobInfo, success);
    }
  }
}
