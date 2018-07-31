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

import com.google.cloud.bigquery.DatasetId;
import com.google.cloud.bigquery.DatasetInfo;
import com.google.cloud.bigquery.TableId;
import com.spotify.flo.EvalContext;
import com.spotify.flo.FloTesting;
import com.spotify.flo.Task;
import com.spotify.flo.TaskBuilder.F0;
import com.spotify.flo.TaskContextStrict;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.threeten.bp.Duration;

public class BigQueryContext extends TaskContextStrict<StagingTableId, TableId> {

  private static final Logger LOG = LoggerFactory.getLogger(BigQueryContext.class);

  private final F0<FloBigQueryClient> bigQuerySupplier;
  private final TableId tableId;

  private transient FloBigQueryClient bigQuery;

  BigQueryContext(F0<FloBigQueryClient> bigQuery, TableId tableId) {
    this.bigQuerySupplier = bigQuery;
    this.tableId = tableId;
  }

  public static BigQueryContext create(String project, String dataset, String table) {
    return create(TableId.of(project, dataset, table));
  }

  public static BigQueryContext create(TableId tableId) {
    return create(BigQueryContext::defaultBigQuerySupplier, tableId);
  }

  static BigQueryContext create(F0<FloBigQueryClient> bigQuerySupplier, TableId tableId) {
    return new BigQueryContext(bigQuerySupplier, tableId);
  }

  public TableId tableId() {
    return tableId;
  }

  private DatasetInfo getDatasetOrThrow() {
    final DatasetId datasetId = DatasetId.of(tableId.getProject(), tableId.getDataset());

    final DatasetInfo dataset = bigQuery().getDataset(datasetId);

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

    if (bigQuery().getDataset(stagingDatasetId) == null) {
      bigQuery().create(DatasetInfo
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

    if (!bigQuery().tableExists(tableId)) {
      return Optional.empty();
    }

    return Optional.of(tableId);
  }

  public static BigQueryMocking mock() {
    return BigQueryMocking.mock();
  }

  TableId publish(StagingTableId stagingTableId) {
    bigQuery().publish(stagingTableId, tableId);
    return tableId;
  }

  private FloBigQueryClient bigQuery() {
    if (bigQuery == null) {
      bigQuery = bigQuerySupplier.get();
    }
    return bigQuery;
  }



  static FloBigQueryClient defaultBigQuerySupplier() {
    if (FloTesting.isTest()) {
      return BigQueryMocking.mock().client();
    } else {
      return BigQueryClientSingleton.BIGQUERY_CLIENT;
    }
  }
}
