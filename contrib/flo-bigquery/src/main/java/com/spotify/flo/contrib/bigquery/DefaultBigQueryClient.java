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

import com.google.cloud.WaitForOption;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQuery.JobOption;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.CopyJobConfiguration;
import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.DatasetId;
import com.google.cloud.bigquery.DatasetInfo;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.TableId;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultBigQueryClient implements FloBigQueryClient {

  private static final Logger LOG = LoggerFactory.getLogger(DefaultBigQueryClient.class);
  private final BigQuery client;

  public DefaultBigQueryClient(BigQuery bigqueryClient) {
    client = bigqueryClient;
  }

  @Override
  public Dataset getDataset(DatasetId datasetId) {
    return client.getDataset(datasetId);
  }

  @Override
  public Dataset create(DatasetInfo datasetInfo) {
    return client.create(datasetInfo);
  }

  @Override
  public boolean tableExists(TableId tableId) {
    return client.getTable(tableId) != null;
  }

  @Override
  public void publish(StagingTableId stagingTableId, TableId tableId) {
    final TableId staging = stagingTableId.tableId();
    LOG.debug("copying staging table {} to {}", staging, tableId);
    try {
      final Job job = client.create(JobInfo.of(CopyJobConfiguration.of(tableId, staging)))
          .waitFor(WaitForOption.timeout(1, TimeUnit.MINUTES));
      throwIfUnsuccessfulJobStatus(job, tableId);
    } catch (BigQueryException e) {
      LOG.error("Could not copy BigQuery table {} from staging to target", tableId, e);
      throw e;
    } catch (InterruptedException | TimeoutException e) {
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
}
