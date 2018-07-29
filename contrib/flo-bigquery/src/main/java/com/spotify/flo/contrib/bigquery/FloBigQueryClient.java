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

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.DatasetId;
import com.google.cloud.bigquery.DatasetInfo;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableId;

/**
 * An interface to BigQuery utilities.
 */
public interface FloBigQueryClient {

  /**
   * Get a dataset by Id
   */
  Dataset getDataset(DatasetId datasetId);

  /**
   * Create a dataset and return a reference to it.
   */
  Dataset create(DatasetInfo datasetInfo);

  /**
   * Create a BigQuery Job with info and options
   * @param jobInfo the job info
   * @param jobOptions other options
   */
  Job create(JobInfo jobInfo, BigQuery.JobOption... jobOptions);

  /**
   * Publish a table by copying from stagingTableId to tableId
   * @param stagingTableId source table id
   * @param tableId destination table id
   */
  void publish(StagingTableId stagingTableId, TableId tableId);

  /**
   * Check if a BigQuery table exists
   * @param tableId
   * @return true if it exists, otherwise false
   */
  boolean tableExists(TableId tableId);
}
