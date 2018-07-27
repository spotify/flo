package com.spotify.flo.contrib.bigquery;

import com.google.cloud.bigquery.BigQuery.JobOption;
import com.google.cloud.bigquery.BigQuery.TableOption;
import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.DatasetId;
import com.google.cloud.bigquery.DatasetInfo;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableId;
import com.spotify.flo.TestContext;

public class BigQueryMocking {

  private static final TestContext.Key<BigQueryMocking> INSTANCE =
      TestContext.key("bigquery-mocking", BigQueryMocking::new);

  private BigQueryMocking() {
  }

  /**
   * Get a {@link BigQueryMocking} instance to be used for mocking bigquery tables and checking
   * tables outputed by a task output. Must be called in a flo test scope.
   */
  static BigQueryMocking mock() {
    return INSTANCE.get();
  }


  public FloBigQueryClient client() {
    return new MockBigQueryClient();
  }

  /**
   * BigQuery mock client that returns mocked datasets and tables and records dataset and table
   * creation and deletion
   */
  private class MockBigQueryClient implements FloBigQueryClient {

    @Override
    public Dataset getDataset(DatasetId datasetId) {
      return null;
    }

    @Override
    public Dataset create(DatasetInfo datasetInfo) {
      return null;
    }

    @Override
    public Table getTable(TableId tableId, TableOption... tableOptions) {
      return null;
    }

    @Override
    public Job create(JobInfo jobInfo, JobOption... jobOptions) {
      return null;
    }

    @Override
    public boolean delete(TableId tableId) {
      return false;
    }
  }
}
