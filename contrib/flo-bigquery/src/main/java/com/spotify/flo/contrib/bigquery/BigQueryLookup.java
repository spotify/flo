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

import com.google.cloud.bigquery.TableId;
import com.spotify.flo.util.Date;
import java.io.Serializable;
import java.util.Objects;
import java.util.Optional;

class BigQueryLookup implements Serializable {

  private static final long serialVersionUID = 1L;

  BigQueryLookup() {
  }

  Spec lookup(TableId tableId) {
    return new LookupSpec(tableId);
  }

  Spec lookupLatestDaily(String project, String dataset, String tableName, Date start, int lookBackDays) {
    return new LookupLatestDailySpec(project, dataset, tableName, start, lookBackDays);
  }

  interface Spec extends Serializable {

    Optional<TableId> lookup(FloBigQueryClient bq);
  }

  static class LookupSpec implements Spec {

    private static final long serialVersionUID = 1L;

    final TableId tableId;

    LookupSpec(TableId tableId) {
      this.tableId = tableId;
    }

    @Override
    public Optional<TableId> lookup(FloBigQueryClient bq) {
      return bq.tableExists(tableId) ? Optional.of(tableId) : Optional.empty();
    }
  }

  static class LookupLatestDailySpec implements Spec {

    private static final long serialVersionUID = 1L;

    final String project;
    final String dataset;
    final String tableName;
    final Date start;
    final int lookBackDays;

    LookupLatestDailySpec(String project, String dataset, String tableName, Date start, final int lookBackDays) {
      this.project = Objects.requireNonNull(project, "project");
      this.dataset = Objects.requireNonNull(dataset, "dataset");
      this.tableName = Objects.requireNonNull(tableName, "tableName");
      this.start = Objects.requireNonNull(start, "start");
      this.lookBackDays = lookBackDays;
    }

    @Override
    public Optional<TableId> lookup(FloBigQueryClient bq) {
      for (int i = 0; i <= lookBackDays; i++) {
        Date date = Date.of(start.localDate().minusDays(i));
        String table = tableName + "_" + BigQueryTasks.formatTableDate(date);
        TableId tableId = TableId.of(project, dataset, table);

        if (bq.tableExists(tableId)) {
          return Optional.of(tableId);
        }
      }

      return Optional.empty();
    }

  }

}
