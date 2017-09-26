/*-
 * -\-\-
 * flo runner
 * --
 * Copyright (C) 2016 Spotify AB
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

package com.spotify.flo.context;

import static com.spotify.flo.TaskContext.async;
import static java.lang.System.getProperty;
import static java.util.Objects.requireNonNull;

import com.google.common.collect.ImmutableList;
import com.google.common.io.Closer;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.spotify.apollo.core.ApolloHelpException;
import com.spotify.apollo.core.Service;
import com.spotify.apollo.core.Services;
import com.spotify.flo.Task;
import com.spotify.flo.TaskConstructor;
import com.spotify.flo.TaskContext;
import com.spotify.flo.TaskInfo;
import com.spotify.flo.freezer.Persisted;
import com.spotify.flo.freezer.PersistingContext;
import com.spotify.flo.status.TaskStatusException;
import com.typesafe.config.Config;
import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ServiceLoader;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * TODO: document.
 */
public final class FloRunner {

  public static final String NAME = "flo-runner";

  private static volatile Logging logging = new ConsoleLogging();
  private static volatile int exitCode = 0;

  private final Service.Instance instance;
  private final Config config;
  private final Closer closer;

  private FloRunner(Service.Instance instance) {
    this.instance = requireNonNull(instance);

    this.config = instance.getConfig();
    this.closer = instance.getCloser();
  }

  public static <T> void runTask(Task<T> task, String... args) {
    final TaskConstructor<T> constructor = new TaskConstructor<T>() {
      @Override
      public String name() {
        return "phony";
      }

      @Override
      public Task<T> create(String... ignored) {
        return task;
      }
    };
    runTask(constructor, args);
  }

  public static void runTask(TaskConstructor<?> taskConstructor, String... args) {
    final Service service = Services.usingName(NAME)
        .withEnvVarPrefix("FLO")
        .build();

    try (Service.Instance i = service.start(args)) {
      final long t0 = System.currentTimeMillis();
      final FloRunner runner = new FloRunner(i);
      final Task<?> task = runner.run(taskConstructor);

      i.waitForShutdown();
      final long elapsed = System.currentTimeMillis() - t0;
      logging.complete(task.id(), elapsed);
    } catch (ApolloHelpException ahe) {
      logging.help(ahe);
      System.exit(1);
    } catch (Throwable e) {
      logging.exception(e);
      System.exit(1);
    }

    System.exit(exitCode);
  }

  private Task<?> run(TaskConstructor<?> taskConstructor) {
    logging = (isMode("tree") && isJsonMode()) ? new JsonTreeLogging() : logging;
    closer.register(logging.init(instance));

    final ImmutableList<String> unprocessedArgs = instance.getUnprocessedArgs();
    final String[] taskArgs = unprocessedArgs.toArray(new String[unprocessedArgs.size()]);

    logging.header();

    final Task<?> task = taskConstructor.create(taskArgs);

    if (isMode("tree")) {
      logging.tree(TaskInfo.ofTask(task));
      exit(0);
    } else {
      final TaskContext taskContext = createContext();
      logging.printPlan(TaskInfo.ofTask(task));
      final TaskContext.Value<?> value = taskContext.evaluate(task);
      value.consume((ˍ) -> exit(0));
      value.onFail(this::exit);
    }

    return task;
  }

  private TaskContext createContext() {
    final ExecutorService executor = Executors.newFixedThreadPool(
        workers(),
        new ThreadFactoryBuilder()
            .setNameFormat("flo-worker-%d")
            .setDaemon(true)
            .build());
    closer.register(executorCloser(executor));

    final TaskContext instrumentedContext = instrument(async(executor));
    final TaskContext baseContext = isMode("persist")
        ? persist(instrumentedContext)
        : instrumentedContext;

    return MemoizingContext.composeWith(LoggingContext.composeWith(baseContext, logging));
  }

  private TaskContext instrument(TaskContext delegate) {
    final ServiceLoader<FloListenerFactory> factories =
        ServiceLoader.load(FloListenerFactory.class);

    InstrumentedContext.Listener listener = new NoopListener();
    for (FloListenerFactory factory : factories) {
      final InstrumentedContext.Listener newListener =
          requireNonNull(factory.createListener(config));
      listener = new ChainedListener(newListener, listener, logging);
    }

    return InstrumentedContext.composeWith(delegate, listener);
  }

  private TaskContext persist(TaskContext delegate) {
    final String stateLocation = config.hasPath("flo.state.location")
                                 ? config.getString("flo.state.location")
                                 : "file://" + getProperty("user.dir");

    final URI basePathUri = URI.create(stateLocation);
    final Path basePath = Paths.get(basePathUri).resolve("run-" + randomAlphaNumeric(4));

    try {
      Files.createDirectories(basePath);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    return new PersistingContext(basePath, delegate);
  }

  private int workers() {
    return config.getInt("flo.workers");
  }

  private boolean isMode(String mode) {
    return mode.equalsIgnoreCase(config.getString("mode"));
  }

  private boolean isJsonMode() {
    return config.getBoolean("jsonTree");
  }

  private void exit(Throwable t) {
    logging.exception(t);
    if (t instanceof TaskStatusException) {
      exit(((TaskStatusException) t).code());
    } else if (t instanceof Persisted) {
      exit(0);
    } else {
      exit(1);
    }
  }

  private void exit(int code) {
    exitCode = code;
    instance.getSignaller().signalShutdown();
  }

  private static Closeable executorCloser(ExecutorService executorService) {
    return () -> {
      executorService.shutdown();

      boolean terminated;
      try {
        terminated = executorService.awaitTermination(10, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        terminated = false;
      }

      if (!terminated) {
        executorService.shutdownNow();
      }
    };
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
