/*-
 * -\-\-
 * flo-bigquery
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
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.CopyJobConfiguration;
import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.DatasetId;
import com.google.cloud.bigquery.DatasetInfo;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.TableId;
import com.spotify.flo.EvalContext;
import com.spotify.flo.Task;
import com.spotify.flo.TaskContextStrict;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.threeten.bp.Duration;

public class BigQueryContext extends TaskContextStrict<StagingTableId, TableId> {

  private static final Logger LOG = LoggerFactory.getLogger(BigQueryContext.class);

  private final BigQuery bigQuery;
  private final TableId tableId;

  private BigQueryContext(BigQuery bigQuery, TableId tableId) {
    this.bigQuery = bigQuery;
    this.tableId = tableId;
  }

  public static BigQueryContext create(String project, String dataset, String table) {
    return create(TableId.of(project, dataset, table));
  }

  public static BigQueryContext create(TableId tableId) {
    return create(BigQueryOptions.getDefaultInstance().getService(), tableId);
  }

  static BigQueryContext create(BigQuery bigQuery, TableId tableId) {
    return new BigQueryContext(bigQuery, tableId);
  }

  public TableId tableId() {
    return tableId;
  }

  private Dataset getDatasetOrThrow() {
    final DatasetId datasetId = DatasetId.of(tableId.getProject(), tableId.getDataset());

    final Dataset dataset = bigQuery.getDataset(datasetId);

    if (dataset == null) {
      LOG.error("Could not find dataset {}", datasetId);
      throw new IllegalArgumentException(
          "Dataset does not exist. Please create it before attempting to write to it.");
    }

    return dataset;
  }

  @Override
  public StagingTableId provide(EvalContext evalContext) {
    final String location = getDatasetOrThrow().getLocation();

    final DatasetId stagingDatasetId = DatasetId.of(tableId.getProject(), "_incoming_" + location);

    if (bigQuery.getDataset(stagingDatasetId) == null) {
      bigQuery.create(DatasetInfo
          .newBuilder(stagingDatasetId)
          .setLocation(location)
          .setDefaultTableLifetime(Duration.ofDays(1).toMillis())
          .build());
      LOG.info("created staging dataset: {}", stagingDatasetId);
    }

    final TableId stagingTableId = TableId.of(
        stagingDatasetId.getProject(),
        stagingDatasetId.getDataset(),
        "_" + tableId.getTable() + "_" + ThreadLocalRandom.current().nextLong(10_000_000));

    return StagingTableId.of(this, stagingTableId);
  }

  @Override
  public Optional<TableId> lookup(Task<TableId> task) {
    getDatasetOrThrow();

    if (bigQuery.getTable(tableId) == null) {
      return Optional.empty();
    }

    return Optional.of(tableId);
  }

  TableId publish(StagingTableId stagingTableId) {
    final TableId staging = stagingTableId.tableId();
    LOG.debug("copying staging table {} to {}", staging, tableId);
    try {
      final Job job = bigQuery.create(JobInfo.of(CopyJobConfiguration.of(tableId, staging)))
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
    bigQuery.delete(staging);

    return tableId;
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
