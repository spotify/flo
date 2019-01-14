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

import static com.spotify.flo.contrib.bigquery.FloBigQueryClient.randomStagingTableId;

import com.google.cloud.RetryOption;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQuery.JobOption;
import com.google.cloud.bigquery.BigQueryError;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.CopyJobConfiguration;
import com.google.cloud.bigquery.DatasetId;
import com.google.cloud.bigquery.DatasetInfo;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableResult;
import java.util.Iterator;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.threeten.bp.Duration;

class DefaultBigQueryClient implements FloBigQueryClient {

  private static final Logger LOG = LoggerFactory.getLogger(DefaultBigQueryClient.class);
  private final BigQuery client;

  public DefaultBigQueryClient(BigQuery bigqueryClient) {
    client = bigqueryClient;
  }

  @Override
  public DatasetInfo getDataset(DatasetId datasetId) {
    return client.getDataset(datasetId);
  }

  @Override
  public DatasetInfo create(DatasetInfo datasetInfo) {
    return client.create(datasetInfo);
  }

  @Override
  public boolean tableExists(TableId tableId) {
    return client.getTable(tableId) != null;
  }

  @Override
  public TableId createStagingTableId(TableId tableId, String location) {
    return randomStagingTableId(tableId, location);
  }

  @Override
  public JobInfo job(JobInfo jobInfo, JobOption... options) {
    Job job = client.create(jobInfo, options);
    while (!job.isDone()) {
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(e);
      }
      job = job.reload();
    }

    final BigQueryError error = job.getStatus().getError();
    if (error != null) {
      throw new BigQueryException(0, "BigQuery job failed: " + error);
    }
    return job;
  }

  @Override
  public void publish(StagingTableId stagingTableId, TableId tableId) {
    final TableId staging = stagingTableId.tableId();
    LOG.debug("copying staging table {} to {}", staging, tableId);
    try {
      final Job job = client.create(JobInfo.of(CopyJobConfiguration.of(tableId, staging)))
          .waitFor(RetryOption.initialRetryDelay(Duration.ofSeconds(1)),
              RetryOption.totalTimeout(Duration.ofMinutes(1L)));
      throwIfUnsuccessfulJobStatus(job, tableId);
    } catch (BigQueryException e) {
      LOG.error("Could not copy BigQuery table {} from staging to target", tableId, e);
      throw e;
    } catch (InterruptedException e) {
      LOG.error("Could not copy BigQuery table {} from staging to target", tableId, e);
      throw new RuntimeException(e);
    }

    LOG.debug("deleting staging table {}", staging);
    client.delete(staging);
  }

  private static void throwIfUnsuccessfulJobStatus(Job job, TableId tableId) {
    if (job != null && job.getStatus().getError() == null) {
      LOG.info("successfully published table {}", tableId);
    } else {
      String error;
      if (job == null) {
        error = "job no longer exists";
      } else {
        error = job.getStatus().getError().toString();
      }
      LOG.error("Could not copy BigQuery table {} from staging to target with error: {}",
          tableId, error);
      throw new RuntimeException(error);
    }
  }

  private static class DefaultQueryResult implements BigQueryResult {

    private final TableResult result;

    private DefaultQueryResult(TableResult result) {
      this.result = Objects.requireNonNull(result, "result");
    }

    @Override
    public Schema schema() {
      return result.getSchema();
    }

    @Override
    public long totalRows() {
      return result.getTotalRows();
    }

    @Override
    public Iterator<FieldValueList> iterator() {
      return result.getValues().iterator();
    }

    public static DefaultQueryResult of(TableResult result) {
      return new DefaultQueryResult(result);
    }
  }
}
