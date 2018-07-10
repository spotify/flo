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

import static java.lang.System.getProperty;
import static java.util.Objects.requireNonNull;

import com.spotify.flo.EvalContext;
import com.spotify.flo.Task;
import com.spotify.flo.TaskInfo;
import com.spotify.flo.freezer.Persisted;
import com.spotify.flo.freezer.PersistingContext;
import com.spotify.flo.status.NotReady;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.io.Closeable;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.URI;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Objects;
import java.util.ServiceLoader;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class implements a top-level runner for {@link Task}s.
 */
public final class FloRunner<T> {

  private static final Logger LOG = LoggerFactory.getLogger(FloRunner.class);

  private static final String MODE = "mode";
  private static final String FLO_ASYNC = "flo.async";
  private static final String FLO_WORKERS = "flo.workers";
  private static final String FLO_FORKING = "flo.forking";
  private static final String FLO_STATE_LOCATION = "flo.state.location";

  private final Logging logging = Logging.create(LOG);
  private final Collection<Closeable> closeables = new ArrayList<>();
  private final Config config;

  private FloRunner(Config config) {
    this.config = requireNonNull(config);
  }

  private static Config defaultConfig() {
    return ConfigFactory.load("flo");
  }

  /**
   * Run task and return an asynchronous {@link Result} containing the last value or throwable.
   * @param task task to run
   * @param config configuration to apply
   * @param <T> type of task
   * @return a {@link Result} with the value and throwable (if thrown)
   */
  public static <T> Result<T> runTask(Task<T> task, Config config) {
    return new Result<>(new FloRunner<T>(config).run(task), loadTerminationHooks(config));
  }

  private static Iterable<TerminationHook> loadTerminationHooks(Config config) {
    final ServiceLoader<TerminationHookFactory> factories =
        ServiceLoader.load(TerminationHookFactory.class);

    return StreamSupport
        .stream(
            Spliterators.spliteratorUnknownSize(factories.iterator(), Spliterator.ORDERED), false)
        .map(factory -> Objects.requireNonNull(factory.create(config)))
        .collect(Collectors.toList());
  }

  /**
   * Run task and return an asynchronous {@link Result} containing the last value or throwable.
   * @param task task to run
   * @param <T> type of task
   * @return a {@link Result} with the value and throwable (if thrown)
   */
  public static <T> Result<T> runTask(Task<T> task) {
    return runTask(task, defaultConfig());
  }

  private Future<T> run(Task<T> task) {
    logging.header();

    if (isMode("tree")) {
      logging.tree(TaskInfo.ofTask(task));
      return CompletableFuture.completedFuture(null);
    }

    logging.printPlan(TaskInfo.ofTask(task));

    final EvalContext evalContext = createContext();
    final long t0 = System.nanoTime();
    final EvalContext.Value<T> value = evalContext.evaluate(task);
    final CompletableFuture<T> future = new CompletableFuture<>();

    value.consume(future::complete);
    value.onFail(future::completeExceptionally);

    return future.handle((v, throwable) -> {
      new Thread(() ->
          closeables.forEach(closeable -> {
            try {
              closeable.close();
            } catch (IOException e) {
              LOG.warn("could not close {}", closeable.getClass(), e);
            }
          }), "flo-runner-closer").start();

      if (throwable != null) {
        logging.exception(throwable);
        logging.complete(task.id(), Duration.ofNanos(System.nanoTime() - t0));
        throw new CompletionException(throwable);
      }

      logging.complete(task.id(), Duration.ofNanos(System.nanoTime() - t0));

      return v;
    });
  }

  private EvalContext createContext() {
    final EvalContext baseContext = instrument(createRootContext());

    if (isMode("persist")) {
      return
          MemoizingContext.composeWith(
              OverridingContext.composeWith(
                  LoggingContext.composeWith(
                      persist(baseContext), logging),
                  logging));
    } else {
      return
          TracingContext.composeWith(
              forkingContext(
                  MemoizingContext.composeWith(
                      OverridingContext.composeWith(
                          LoggingContext.composeWith(
                              baseContext, logging),
                          logging))));
    }
  }

  private EvalContext createRootContext() {
    if (config.getBoolean(FLO_ASYNC)) {
      final AtomicLong count = new AtomicLong(0);
      final ThreadFactory threadFactory = runnable -> {
        final Thread thread = Executors.defaultThreadFactory().newThread(runnable);
        thread.setName("flo-worker-" + count.getAndIncrement());
        thread.setDaemon(true);
        return thread;
      };
      final ExecutorService executor = Executors.newFixedThreadPool(
          config.getInt(FLO_WORKERS),
          threadFactory);
      closeables.add(executorCloser(executor));
      return EvalContext.async(executor);
    } else {
      return EvalContext.sync();
    }
  }

  private EvalContext instrument(EvalContext delegate) {
    final ServiceLoader<FloListenerFactory> factories =
        ServiceLoader.load(FloListenerFactory.class);

    InstrumentedContext.Listener listener = new NoopListener();
    for (FloListenerFactory factory : factories) {
      final InstrumentedContext.Listener newListener =
          requireNonNull(factory.createListener(config));
      listener = new ChainedListener(newListener, listener, logging);
    }

    closeables.add(listener);
    return InstrumentedContext.composeWith(delegate, listener);
  }

  private EvalContext forkingContext(EvalContext baseContext) {
    final boolean inDebugger = ManagementFactory.getRuntimeMXBean()
        .getInputArguments().stream().anyMatch(s -> s.contains("-agentlib:jdwp"));

    if (hasExplicitConfigValue(FLO_FORKING)) {
      if (config.getBoolean(FLO_FORKING)) {
        LOG.debug("Forking enabled (config variable flo.forking=true)");
        return ForkingContext.composeWith(baseContext);
      } else {
        LOG.debug("Forking disabled (config variable flo.forking=false)");
        return baseContext;
      }
    } else if (inDebugger) {
      LOG.debug("Debugger detected, forking disabled by default "
          + "(enable by setting config variable flo.forking=true)");
      return baseContext;
    } else {
      LOG.debug("Debugger not detected, forking enabled by default "
          + "(disable by setting config variable flo.forking=false)");
      return ForkingContext.composeWith(baseContext);
    }
  }

  private EvalContext persist(EvalContext delegate) {
    final String stateLocation = config.hasPath(FLO_STATE_LOCATION)
                                 ? config.getString(FLO_STATE_LOCATION)
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

  private boolean isMode(String mode) {
    return mode.equalsIgnoreCase(config.getString(MODE));
  }

  private boolean hasExplicitConfigValue(String path) {
    final URL configUrl = config.getValue(path).origin().url();

    // If set through env var or system property, there will be no url
    return configUrl == null || !configUrl.getFile().endsWith("reference.conf");

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

  public static class Result<T> {

    private final Future<T> future;
    private final Iterable<TerminationHook> terminationHooks;

    Result(Future<T> future, Iterable<TerminationHook> terminationHooks) {
      this.future = future;
      this.terminationHooks = terminationHooks;
    }

    public Future<T> future() {
      return future;
    }

    /**
     * Wait until task has finished running and {@code System.exit()} exits
     * with an appropriate status code.
     */
    public void waitAndExit() {
      waitAndExit(System::exit);
    }

    /**
     * See {@link Future#get()}
     */
    public T value() throws ExecutionException, InterruptedException {
      return future.get();
    }

    // visible for testing
    void waitAndExit(Consumer<Integer> exiter) {
      try {
        future.get();
        exit(exiter, 0);
      } catch (ExecutionException e) {
        final int status;
        if (e.getCause() instanceof NotReady) {
          status = 20;
        } else if (e.getCause() instanceof Persisted) {
          status = 0;
        } else {
          status = 1;
        }
        exit(exiter, status);
      } catch (RuntimeException | InterruptedException e) {
        exit(exiter, 1);
      }
    }

    private void exit(Consumer<Integer> exiter, int exitCode) {
      this.terminationHooks.forEach(hook -> {
        try {
          hook.accept(exitCode);
        } catch (Exception e) {
          LOG.warn("Termination hook failed ", e);
        }
      });
      exiter.accept(exitCode);
    }
  }
}
