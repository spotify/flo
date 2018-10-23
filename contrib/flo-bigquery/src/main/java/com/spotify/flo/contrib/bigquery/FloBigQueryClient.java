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

import com.google.cloud.bigquery.BigQuery.JobOption;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.DatasetId;
import com.google.cloud.bigquery.DatasetInfo;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.TableId;
import java.util.concurrent.ThreadLocalRandom;

/**
 * An interface to BigQuery utilities.
 */
interface FloBigQueryClient {

  /**
   * Get a dataset by Id
   */
  DatasetInfo getDataset(DatasetId datasetId);

  /**
   * Create a dataset and return a reference to it.
   */
  DatasetInfo create(DatasetInfo datasetInfo);

  /**
   * Publish a table by copying from stagingTableId to tableId
   *
   * @param stagingTableId source table id
   * @param tableId destination table id
   */
  void publish(StagingTableId stagingTableId, TableId tableId);

  /**
   * Check if a BigQuery table exists
   *
   * @return true if it exists, otherwise false
   */
  boolean tableExists(TableId tableId);

  /**
   * Run a BigQuery query. Blocks until the query completes.
   *
   * @throws BigQueryException if the query fails.
   */
  BigQueryResult query(QueryJobConfiguration queryRequest);

  /**
   * Run a BiqQuery job. Blocks until the job completes.
   *
   * @throws BigQueryException if the job fails.
   */
  JobInfo job(JobInfo jobInfo, JobOption... options);

  /**
   * Create a staging {@link TableId} for {@param tableId}
   */
  TableId createStagingTableId(TableId tableId, String location);

  /**
   * Create a random staging table id.
   */
  static TableId randomStagingTableId(TableId tableId, String location) {
    final DatasetId stagingDatasetId = DatasetId.of(tableId.getProject(), "_incoming_" + location);
    final String table = "_" + tableId.getTable() + "_" + ThreadLocalRandom.current().nextLong(10_000_000);
    return TableId.of(stagingDatasetId.getProject(), stagingDatasetId.getDataset(), table);
  }
}
