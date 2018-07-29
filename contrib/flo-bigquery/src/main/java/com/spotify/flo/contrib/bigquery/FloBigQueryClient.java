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
