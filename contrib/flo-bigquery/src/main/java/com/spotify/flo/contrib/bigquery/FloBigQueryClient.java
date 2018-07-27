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
   * Get a table by Id
   * @param tableId the table Id
   * @param tableOptions other table options
   */
  Table getTable(TableId tableId, BigQuery.TableOption... tableOptions);

  /**
   * Create a BigQuery Job with info and options
   * @param jobInfo the job info
   * @param jobOptions other options
   */
  Job create(JobInfo jobInfo, BigQuery.JobOption... jobOptions);

  /**
   * Delete a table by Id
   * @param tableId
   */
  boolean delete(TableId tableId);

}
