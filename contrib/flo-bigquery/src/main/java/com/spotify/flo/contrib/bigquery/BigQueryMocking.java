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

import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.DatasetId;
import com.google.cloud.bigquery.DatasetInfo;
import com.google.cloud.bigquery.TableId;
import com.spotify.flo.TestContext;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListSet;

public class BigQueryMocking {

  // TODO: should be possible for tests to distinguish between tables mocked by user and tables created by task

  private static final TestContext.Key<BigQueryMocking> INSTANCE =
      TestContext.key("bigquery-mocking", BigQueryMocking::new);
  private final ConcurrentMap<DatasetId, ConcurrentSkipListSet<String>> productionTables = new ConcurrentHashMap<>();

  private BigQueryMocking() {
  }

  /**
   * Get a {@link BigQueryMocking} instance to be used for mocking bigquery tables and checking
   * tables outputed by a task output. Must be called in a flo test scope.
   */
  public static BigQueryMocking mock() {
    return INSTANCE.get();
  }


  public FloBigQueryClient client() {
    return new MockBigQueryClient();
  }

  public boolean tableExists(TableId tableId) {
    return Optional.ofNullable(productionTables.get(datasetIdOf(tableId)))
        .map(tables -> tables.contains(tableId.getTable()))
        .orElse(false);
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
    public DatasetInfo getDataset(DatasetId datasetId) {
      return Dataset.newBuilder(datasetId)
          .setLocation("test") // TOOD: make mockable?
          .build();
    }

    @Override
    public DatasetInfo create(DatasetInfo datasetInfo) {
      return datasetInfo;
    }

    @Override
    public boolean tableExists(TableId tableId) {
      return BigQueryMocking.this.tableExists(tableId);
    }

    @Override
    public void publish(StagingTableId stagingTableId, TableId tableId) {
      final DatasetId datasetId = datasetIdOf(tableId);
      productionTables.computeIfAbsent(datasetId, k -> new ConcurrentSkipListSet<>())
          .add(tableId.getTable());
    }
  }
}
