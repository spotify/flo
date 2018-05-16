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
import java.util.Objects;

public class StagingTableId {

  private final BigQueryContext bigQueryContext;
  private final TableId tableId;

  private StagingTableId(BigQueryContext bigQueryContext, TableId tableId) {
    this.bigQueryContext = Objects.requireNonNull(bigQueryContext);
    this.tableId = Objects.requireNonNull(tableId);
  }

  static StagingTableId of(BigQueryContext bigQueryContext, TableId tableId) {
    return new StagingTableId(bigQueryContext, tableId);
  }

  public TableId tableId() {
    return tableId;
  }

  public TableId publish() {
    return bigQueryContext.publish(this);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final StagingTableId that = (StagingTableId) o;

    return tableId.equals(that.tableId);
  }

  @Override
  public int hashCode() {
    return tableId.hashCode();
  }
}
