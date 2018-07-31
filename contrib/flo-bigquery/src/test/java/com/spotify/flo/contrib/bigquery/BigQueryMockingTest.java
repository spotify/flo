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

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import com.google.cloud.bigquery.TableId;
import com.spotify.flo.FloTesting;
import com.spotify.flo.Task;
import com.spotify.flo.TestScope;
import com.spotify.flo.context.FloRunner;
import java.util.concurrent.TimeUnit;
import org.junit.Test;

public class BigQueryMockingTest {

  @Test
  public void lookupShouldReturnMockedTable() throws Exception {
    final Task<TableId> lookup = BigQueryTasks.lookup("foo", "bar", "tab");
    try (TestScope scope = FloTesting.scope()) {
      BigQueryMocking.mock().table("foo", "bar", "tab");
      final TableId tableId = FloRunner.runTask(lookup).future().get(30, TimeUnit.SECONDS);
      assertThat(tableId, is(TableId.of("foo","bar","tab")));
    }
  }

  @Test
  public void contextShouldLookupMockedTableAndNotRunProcessFn() throws Exception {

    final TableId expected = TableId.of("foo", "bar", "baz");

    final Task<TableId> task = Task.named("task")
        .ofType(TableId.class)
        .context(BigQueryContext.create(expected))
        .process(table -> {
          throw new AssertionError();
        });

    try (TestScope scope = FloTesting.scope()) {
      BigQueryMocking.mock().table(expected);
      final TableId tableId = FloRunner.runTask(task).future().get(30, TimeUnit.SECONDS);
      assertThat(tableId, is(expected));
    }
  }

  @Test
  public void contextShouldCreateTable() throws Exception {

    final TableId expected = TableId.of("foo", "bar", "baz");

    final Task<TableId> task = Task.named("task")
        .ofType(TableId.class)
        .context(BigQueryContext.create(expected))
        .process(StagingTableId::publish);

    try (TestScope scope = FloTesting.scope()) {
      final TableId tableId = FloRunner.runTask(task).future().get(30, TimeUnit.SECONDS);
      assertThat(tableId, is(expected));
      assertThat(BigQueryMocking.mock().tableExists(expected), is(true));
    }
  }
}
