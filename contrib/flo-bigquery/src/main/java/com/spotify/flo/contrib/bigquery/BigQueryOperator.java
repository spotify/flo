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

import static com.spotify.flo.contrib.bigquery.BigQueryClientSingleton.bq;

import com.google.cloud.bigquery.CopyJobConfiguration;
import com.google.cloud.bigquery.ExtractJobConfiguration;
import com.google.cloud.bigquery.JobConfiguration;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.LoadJobConfiguration;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.QueryRequest;
import com.google.common.collect.ImmutableMap;
import com.spotify.flo.EvalContext;
import com.spotify.flo.TaskOperator;
import com.spotify.flo.contrib.bigquery.BigQueryOperation.Provider;
import java.util.function.Supplier;

public class BigQueryOperator<T> implements TaskOperator<BigQueryOperation.Provider<T>, BigQueryOperation<T, ?>, T> {

  private static final long serialVersionUID = 1L;

  private BigQueryOperator() {
  }

  @Override
  public T perform(BigQueryOperation<T, ?> spec, Listener listener) {
    final Supplier<Object> result;
    final JobId jobId;
    final String jobType;

    // Start job
    if (spec.queryRequest != null) {
      final QueryRequest request = spec.queryRequest.get();
      jobId = bq().startQuery(request);
      jobType = "query";
      result = () -> bq().awaitQueryCompletion(jobId);
    } else if (spec.jobRequest != null) {
      final JobInfo request = spec.jobRequest.get();
      final JobInfo job = bq().startJob(request);
      jobId = job.getJobId();
      jobType = bqJobType(request);
      result = () -> bq().awaitJobCompletion(job);
    } else {
      throw new AssertionError();
    }

    // Emit metadata
    listener.meta(spec.taskId, ImmutableMap.of(
        "job-type", "bigquery",
        "job-id", jobId.getJob(),
        "bq-job-type", jobType,
        "project-id", bq().projectId()));

    // Await result
    return spec.success.apply(result.get());
  }

  private static String bqJobType(JobInfo jobInfo) {
    final JobConfiguration configuration = jobInfo.getConfiguration();
    if (configuration instanceof CopyJobConfiguration) {
      return "copy";
    } else if (configuration instanceof ExtractJobConfiguration) {
      return "extract";
    } else if (configuration instanceof LoadJobConfiguration) {
      return "load";
    } else if (configuration instanceof QueryJobConfiguration) {
      return "query";
    } else {
      return "unknown";
    }
  }

  public static <T> BigQueryOperator<T> create() {
    return new BigQueryOperator<>();
  }

  @Override
  public Provider<T> provide(EvalContext evalContext) {
    return new BigQueryOperation.Provider<>(evalContext.currentTask().get().id());
  }
}
