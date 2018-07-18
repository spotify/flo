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

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import com.google.cloud.bigquery.TableId;
import com.google.common.base.Throwables;
import com.spotify.flo.Task;
import com.spotify.flo.context.FloRunner;
import com.spotify.flo.freezer.PersistingContext;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import org.junit.Test;

public class BigQueryTasksTest {

  @Test
  public void lookupShouldBeSerializable() {
    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    final Task<TableId> task = BigQueryTasks.lookup("foo", "bar", "baz");
    PersistingContext.serialize(task, baos);
    final ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
    final Task<TableId> deserializedTask = PersistingContext.deserialize(bais);
    assertThat(deserializedTask, is(notNullValue()));
  }

  @Test
  public void lookupShouldBeRunnable() throws Exception {
    final String rock = Long.toHexString(ThreadLocalRandom.current().nextLong());
    final Future<TableId> future = FloRunner.runTask(BigQueryTasks.lookup(() -> {
      throw new UnsupportedOperationException(rock);
    }, TableId.of("foo", "bar", "baz"))).future();

    try {
      future.get();
      fail();
    } catch (ExecutionException e) {
      // Verify that we are getting the expected root cause, not some serialization error etc
      final Throwable rootCause = Throwables.getRootCause(e);
      assertThat(rootCause, instanceOf(UnsupportedOperationException.class));
      assertThat(rootCause.getMessage(), is(rock));
    }
  }
}