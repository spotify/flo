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

import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.QueryRequest;
import com.spotify.flo.Fn;
import com.spotify.flo.TaskBuilder.F1;
import com.spotify.flo.TaskId;
import java.io.Serializable;
import java.util.Objects;

/**
 * A BigQuery operation to be executed by the {@link BigQueryOperator}.
 */
public class BigQueryOperation<T, R> implements Serializable {

  private static final long serialVersionUID = 2L;

  final TaskId taskId;

  Fn<JobInfo> jobRequest;
  Fn<QueryRequest> queryRequest;
  F1<Object, T> success;

  private BigQueryOperation(TaskId taskId) {
    this.taskId = Objects.requireNonNull(taskId, "taskId");
  }

  /**
   * Run a query. Result are returned directly and not written to a table.
   */
  @SuppressWarnings("unchecked")
  BigQueryOperation<T, BigQueryResult> query(Fn<QueryRequest> queryRequest) {
    if (jobRequest != null) {
      throw new IllegalStateException("can only run either a query or a job");
    }
    this.queryRequest = Objects.requireNonNull(queryRequest);
    return (BigQueryOperation<T, BigQueryResult>) this;
  }

  /**
   * Run a job. This can be a query, copy, load or extract with results written to a table, etc.
   */
  @SuppressWarnings("unchecked")
  BigQueryOperation<T, JobInfo> job(Fn<JobInfo> jobRequest) {
    if (queryRequest != null) {
      throw new IllegalStateException("can only run either a query or a job");
    }
    this.jobRequest = Objects.requireNonNull(jobRequest);
    return (BigQueryOperation<T, JobInfo>) this;
  }

  /**
   * Specify some action to take on success. E.g. publishing a staging table.
   */
  @SuppressWarnings("unchecked")
  BigQueryOperation<T, R> success(F1<R, T> success) {
    this.success = (F1<Object, T>) Objects.requireNonNull(success);
    return this;
  }

  public static class Provider<T> implements Serializable {

    private static final long serialVersionUID = 2L;

    final TaskId taskId;

    Provider(TaskId taskId) {
      this.taskId = taskId;
    }

    public BigQueryOperation<T, Object> bq() {
      return new BigQueryOperation<>(taskId);
    }

    public BigQueryOperation<T, BigQueryResult> query(Fn<QueryRequest> queryRequest) {
      return new BigQueryOperation<T, BigQueryResult>(taskId).query(queryRequest);
    }

    public BigQueryOperation<T, BigQueryResult> query(QueryRequest queryRequest) {
      return new BigQueryOperation<T, BigQueryResult>(taskId).query(() -> queryRequest);
    }

    public BigQueryOperation<T, BigQueryResult> query(String query) {
      return new BigQueryOperation<T, BigQueryResult>(taskId).query(() -> QueryRequest.of(query));
    }

    public BigQueryOperation<T, JobInfo> job(Fn<JobInfo> jobInfo) {
      return new BigQueryOperation<T, JobInfo>(taskId).job(jobInfo);
    }

    public BigQueryOperation<T, JobInfo> job(JobInfo jobInfo) {
      return new BigQueryOperation<T, JobInfo>(taskId).job(() -> jobInfo);
    }
  }
}
