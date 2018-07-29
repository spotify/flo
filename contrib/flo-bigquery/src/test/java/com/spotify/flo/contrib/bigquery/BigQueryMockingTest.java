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
  public void shouldReturnMockedTable() throws Exception {

    final Task<TableId> lookup = BigQueryTasks.lookup("foo", "bar", "tab");

    try (TestScope scope = FloTesting.scope()) {
      BigQueryMocking.mock().table("foo", "bar", "tab");

      final TableId tableId = FloRunner.runTask(lookup).future().get(30, TimeUnit.SECONDS);

      assertThat(tableId, is(TableId.of("foo","bar","tab")));
    }
  }


}
