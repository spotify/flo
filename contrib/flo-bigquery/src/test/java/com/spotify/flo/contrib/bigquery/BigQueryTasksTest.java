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
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.cloud.bigquery.TableId;
import com.google.common.base.Throwables;
import com.spotify.flo.Task;
import com.spotify.flo.context.FloRunner;
import com.spotify.flo.freezer.PersistingContext;
import com.spotify.flo.status.NotReady;
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
    final Future<TableId> future = FloRunner.runTask(BigQueryTasks.lookup(
        "none-existent-project", "none-existent-dataset", "none-existent-table")).future();

    try {
      future.get();
      fail();
    } catch (ExecutionException e) {
      // Verify that we are getting some well known error here so we know with some
      // certainty that we didn't get a serialization error. Yes, this is quite awful.
      final Throwable rootCause = Throwables.getRootCause(e);
      if (rootCause instanceof NotReady) {
        // Seems we had working credentials and the lookup worked. We're done here.
      } else if (rootCause instanceof GoogleJsonResponseException) {
        // Seems we managed to make a request, so the lookup executed. We're done here.
      } else if (rootCause instanceof IllegalArgumentException &&
          rootCause.getMessage().startsWith("A project ID is required")) {
        // Seems we managed to get as far as trying to instantiate the BigQuery client (in the task process).
        // We're done here.
      } else {
        // Not sure what went wrong here, might be serialization error, so be conservative and fail here.
        throw e;
      }
    }
  }
}
