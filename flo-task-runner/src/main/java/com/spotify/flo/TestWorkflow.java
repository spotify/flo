/*-
 * -\-\-
 * flo-task-runner
 * --
 * Copyright (C) 2016 - 2017 Spotify AB
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

package com.spotify.flo;

import static com.spotify.flo.TaskContext.inmem;
import static com.spotify.flo.Util.colored;
import static java.lang.System.getProperty;
import static java.lang.System.getenv;

import com.spotify.flo.TaskContext.Value;
import com.spotify.flo.context.AwaitingConsumer;
import com.spotify.flo.context.MemoizingContext;
import com.spotify.flo.freezer.Persisted;
import com.spotify.flo.freezer.PersistingContext;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestWorkflow {

  private static final Logger LOG = LoggerFactory.getLogger(TestWorkflow.class);
  private static final String FLO_STATE_LOCATION = "FLO_STATE_LOCATION";

  public static void main(String[] args) throws InterruptedException {
    Task<Long> fib9 = fib(9);
    persist(fib9);
  }

  public static Task<Long> fib(long n) {
    TaskBuilder<Long> fib = Task.named("Fib", n).ofType(Long.class);
    if (n < 2) {
      return fib
          .process(() -> n);
    } else {
      return fib
          .in(() -> fib(n - 1))
          .in(() -> fib(n - 2))
          .process(TestWorkflow::fib);
    }
  }

  static long fib(long a, long b) {
    LOG.info("Fib.process(" + a + " + " + b + ") = " + (a + b));
    return a + b;
  }

  private static void persist(Task<?> task) throws InterruptedException {
    final String cwd = Optional.ofNullable(getenv(FLO_STATE_LOCATION))
        .orElseGet(() -> "file://" + getProperty("user.dir"));
    final URI basePathUri = URI.create(cwd);
    final Path basePath = Paths.get(basePathUri).resolve("run-" + randomAlphaNumeric(4));

    try {
      Files.createDirectories(basePath);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    LOG.info("Persisting tasks DAG to {}", basePath.toUri());

    PersistingContext persistingContext = new PersistingContext(basePath, inmem());
    TaskContext context = MemoizingContext.composeWith(persistingContext);

    AwaitingConsumer<Throwable> await = AwaitingConsumer.create();
    Value<?> value = context.evaluate(task);
    value.onFail(await);

    await.await(1, TimeUnit.MINUTES);
    if (!await.isAvailable()) {
      throw new RuntimeException("Failed to persist");
    }

    if (!(await.get() instanceof Persisted)) {
      throw new RuntimeException(await.get());
    }

    Map<TaskId, Path> files = persistingContext.getFiles();
    files.forEach((taskId, file) ->
        LOG.info("{} -> {}", colored(taskId), file.toUri())
    );
  }

  private static final String ALPHA_NUMERIC_STRING = "abcdefghijklmnopqrstuvwxyz0123456789";

  public static String randomAlphaNumeric(int count) {
    StringBuilder builder = new StringBuilder();
    while (count-- != 0) {
      int character = (int)(Math.random() * ALPHA_NUMERIC_STRING.length());
      builder.append(ALPHA_NUMERIC_STRING.charAt(character));
    }
    return builder.toString();
  }
}
