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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.junit.Test;

public class BigQueryMockingTest {

  @Test
  public void lookupShouldReturnMockedTable() throws Exception {
    final Task<TableId> lookup = BigQueryTasks.lookup("foo", "bar", "tab");
    try (TestScope scope = FloTesting.scope()) {
      assertThat(BigQueryMocking.mock().tablePublished("foo", "bar", "tab"), is(false));
      assertThat(BigQueryMocking.mock().tableExists("foo", "bar", "tab"), is(false));
      BigQueryMocking.mock().table("foo", "bar", "tab");
      assertThat(BigQueryMocking.mock().tablePublished("foo", "bar", "tab"), is(false));
      assertThat(BigQueryMocking.mock().tableExists("foo", "bar", "tab"), is(true));
      final TableId tableId = FloRunner.runTask(lookup).future().get(30, TimeUnit.SECONDS);
      assertThat(tableId, is(TableId.of("foo", "bar", "tab")));
      assertThat(BigQueryMocking.mock().tablePublished("foo", "bar", "tab"), is(false));
      assertThat(BigQueryMocking.mock().tableExists("foo", "bar", "tab"), is(true));
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
      assertThat(BigQueryMocking.mock().tablePublished(expected), is(false));
      assertThat(BigQueryMocking.mock().tableExists(expected), is(false));
      BigQueryMocking.mock().table(expected);
      assertThat(BigQueryMocking.mock().tablePublished(expected), is(false));
      assertThat(BigQueryMocking.mock().tableExists(expected), is(true));
      final TableId tableId = FloRunner.runTask(task).future().get(30, TimeUnit.SECONDS);
      assertThat(tableId, is(expected));
      assertThat(BigQueryMocking.mock().tablePublished(expected), is(false));
      assertThat(BigQueryMocking.mock().tableExists(expected), is(true));
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
      assertThat(BigQueryMocking.mock().tableExists(expected), is(false));
      assertThat(BigQueryMocking.mock().tablePublished(expected), is(false));
      final TableId tableId = FloRunner.runTask(task).future().get(30, TimeUnit.SECONDS);
      assertThat(tableId, is(expected));
      assertThat(BigQueryMocking.mock().tableExists(expected), is(true));
      assertThat(BigQueryMocking.mock().tablePublished(expected), is(true));
    }
  }

  @Test
  public void shouldCreateCustomStagingTableId()
      throws InterruptedException, ExecutionException, TimeoutException {
    final TableId expectedFinal = TableId.of("foo", "bar", "tab");
    final TableId expectedStaging = TableId.of("test_foo", "test_bar", "test_tab");


    final Task<TableId> task = Task.named("task")
        .ofType(TableId.class)
        .context(BigQueryContext.create(expectedFinal))
        .process(actual -> {
          assertThat(actual.tableId(), is(expectedStaging));
          return actual.publish();
        });

    try(final TestScope scope = FloTesting.scope()){
      BigQueryMocking.mock().stagingTableId(expectedFinal, expectedStaging);

      assertThat(BigQueryMocking.mock().tableExists(expectedFinal), is(false));
      assertThat(BigQueryMocking.mock().tablePublished(expectedFinal), is(false));
      assertThat(BigQueryMocking.mock().tableExists(expectedStaging), is(true));
      assertThat(BigQueryMocking.mock().tablePublished(expectedStaging), is(false));

      final TableId resultTableId = FloRunner.runTask(task).future().get(30, TimeUnit.SECONDS);

      assertThat(resultTableId, is(expectedFinal));
      assertThat(BigQueryMocking.mock().tableExists(expectedFinal), is(true));
      assertThat(BigQueryMocking.mock().tablePublished(expectedFinal), is(true));

      assertThat(BigQueryMocking.mock().tableExists(expectedStaging), is(false));
      assertThat(BigQueryMocking.mock().tablePublished(expectedStaging), is(false));
    }
  }
}
