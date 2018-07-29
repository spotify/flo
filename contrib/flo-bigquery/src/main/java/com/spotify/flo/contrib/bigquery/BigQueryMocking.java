package com.spotify.flo.contrib.bigquery;

import com.google.cloud.bigquery.BigQuery.JobOption;
import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.DatasetId;
import com.google.cloud.bigquery.DatasetInfo;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableId;
import com.spotify.flo.TestContext;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListSet;

public class BigQueryMocking {

  private static final TestContext.Key<BigQueryMocking> INSTANCE =
      TestContext.key("bigquery-mocking", BigQueryMocking::new);
  private final ConcurrentMap<DatasetId, ConcurrentSkipListSet<String>> productionTables = new ConcurrentHashMap<>();
  private final ConcurrentMap<DatasetId, ConcurrentSkipListSet<String>> stagingTables = new ConcurrentHashMap<>();

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

  public boolean tableExists(TableId tableId) {
    final ConcurrentSkipListSet<String> tableIds = productionTables.get(datasetIdOf(tableId));
    return tableIds != null && tableIds.contains(tableId.getTable());
  }

  public boolean tableExists(String project, String dataset, String table) {
    return tableExists(TableId.of(project, dataset, table));
  }

  public void dataset(String project, String dataset) {
    dataset(DatasetId.of(project, dataset));
  }

  public void dataset(TableId tableId) {
    dataset(datasetIdOf(tableId));
  }

  public void dataset(DatasetId datasetId) {
    productionTables.putIfAbsent(datasetId, new ConcurrentSkipListSet<>());
  }

  public void table(String project, String dataset, String table) {
    table(TableId.of(project, dataset, table));
  }

  public void table(TableId tableId) {
    dataset(tableId);
    productionTables.get(datasetIdOf(tableId)).add(tableId.getTable());
  }

  private static DatasetId datasetIdOf(TableId tableId) {
    return DatasetId.of(tableId.getProject(), tableId.getDataset());
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
    public boolean tableExists(TableId tableId) {
      return productionTables.getOrDefault(datasetIdOf(tableId), new ConcurrentSkipListSet<>())
          .contains(tableId.getTable());
    }

    @Override
    public Job create(JobInfo jobInfo, JobOption... jobOptions) {
      return null;
    }

    @Override
    public void publish(StagingTableId stagingTableId, TableId tableId) {
      stagingTables.computeIfPresent(datasetIdOf(stagingTableId.tableId()), (key, value) -> {
        //add to 'production' map
        productionTables.putIfAbsent(key, new ConcurrentSkipListSet<>());
        productionTables.get(key).add(tableId.getTable());

        //remove from 'staging' map
        stagingTables.get(key).remove(stagingTableId.tableId().getTable());
        return stagingTables.get(key);
      });
    }
  }
}
