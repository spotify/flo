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

import com.google.cloud.bigquery.TableId;
import com.google.common.annotations.VisibleForTesting;
import com.spotify.flo.Task;
import com.spotify.flo.TaskBuilder.F0;
import com.spotify.flo.status.NotReady;
import com.spotify.flo.util.Date;
import com.spotify.flo.util.DateHour;

public final class BigQueryTasks {

  private BigQueryTasks() {
    throw new UnsupportedOperationException();
  }

  @VisibleForTesting
  static Task<TableId> lookup(F0<FloBigQueryClient> bigQuerySupplier, TableId tableId) {
    return Task.named("bigquery.lookup", tableId.getProject(), tableId.getDataset(), tableId.getTable())
        .ofType(TableId.class)
        .process(() -> {
          if (bigQuerySupplier.get().tableExists(tableId)) {
            return tableId;
          } else {
            throw new NotReady();
          }
        });
  }

  public static Task<TableId> lookup(TableId tableId) {
    return lookup(BigQueryClientSingleton::bq, tableId);
  }

  public static Task<TableId> lookup(String project, String dataset, String table) {
    return lookup(TableId.of(project, dataset, table));
  }

  public static String formatTableDate(Date date) {
    return date.toString().replace("-", "");
  }

  public static String formatTableDateHour(DateHour dateHour) {
    return dateHour.toString().replace("-", "");
  }

  public static String legacyTableRef(TableId tableId) {
    return tableId.getProject() + ":" + tableId.getDataset() + "." + tableId.getTable();
  }

  public static String tableRef(TableId tableId) {
    return tableId.getProject() + "." + tableId.getDataset() + "." + tableId.getTable();
  }
}
