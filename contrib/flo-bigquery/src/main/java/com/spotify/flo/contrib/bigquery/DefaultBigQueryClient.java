package com.spotify.flo.contrib.bigquery;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQuery.JobOption;
import com.google.cloud.bigquery.BigQuery.TableOption;
import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.DatasetId;
import com.google.cloud.bigquery.DatasetInfo;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableId;
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
  public Table getTable(TableId tableId, TableOption... tableOptions) {
    return client.getTable(tableId, tableOptions);
  }

  @Override
  public Job create(JobInfo jobInfo, JobOption... jobOptions) {
    return client.create(jobInfo, jobOptions);
  }

  @Override
  public boolean delete(TableId tableId) {
    return client.delete(tableId);
  }
}
