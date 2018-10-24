/*-
 * -\-\-
 * Flo Runner
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

package com.spotify.flo.context;

import static com.spotify.flo.freezer.PersistingContext.deserialize;
import static com.spotify.flo.freezer.PersistingContext.serialize;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.spotify.flo.EvalContext;
import com.spotify.flo.Fn;
import com.spotify.flo.OperationExtractingContext;
import com.spotify.flo.OperationExtractingContext.Operation;
import com.spotify.flo.Task;
import com.spotify.flo.TaskBuilder.F1;
import com.spotify.flo.TaskBuilder.F2;
import com.spotify.flo.TaskId;
import com.spotify.flo.TaskOperator;
import com.spotify.flo.TaskOperator.Listener;
import com.spotify.flo.TaskOperator.Operation.Result;
import com.spotify.flo.TaskOutput;
import com.spotify.flo.freezer.EvaluatingContext;
import com.spotify.flo.freezer.PersistingContext;
import com.spotify.flo.util.Date;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A POC for ephemeral workflow execution.
 *
 * <p>The task graph and all operations are persisted to a database.
 *
 * <p>The workflow is executed by looping over all tasks and evaluating them when their inputs are ready. If a task
 * has a {@link TaskOperator}, the operation is started and persisted to the database.
 *
 * <p>Operations are polled for completion. When an operation is completed, the task is completed with the output of
 * the operation.
 *
 * <p>The workflow has completed when all tasks and operations have completed.
 *
 * <p>Each workflow execution iteration takes place in a new subprocess. No state is kept in memory.
 */
public class EphemeralExecutionTest {

  private static final boolean DEBUG = true;

  private static final String ROOT_TASK_ID = "root_task_id";
  private static final String TASK_PATH = "task_path";
  private static final String TASK = "task";
  private static final String OPERATION = "operation";
  private static final String OPERATION_STATE = "operation_states";
  private static final String OPERATION_SCHEDULE = "operation_schedule";

  private static final Logger log = LoggerFactory.getLogger(EphemeralExecutionTest.class);
  private static final F2<String, String, TaskOperator.Operation> ASSERTION_ERROR = (type, key) -> {
    throw new AssertionError("Value for '" + type + "|" + key + "' not found");
  };

  @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void test() throws Exception {
    final String dbPath = temporaryFolder.newFolder().getAbsolutePath();
    final String persistedTasksDir = temporaryFolder.newFolder().getAbsolutePath();

    log.info("db path: {}", dbPath);

    // Persist task graph
    log.info("Persisting workflow to {}", persistedTasksDir);
    fork(() -> {
      persistWorkflow(dbPath, persistedTasksDir);
      return null;
    });

    for (int i = 0; ; i++) {
      log.info("Executing workflow (iteration={})", i);
      final Tick tick = fork(() -> runWorkflowTick(dbPath, persistedTasksDir));
      if (!tick.isDone()) {
        final long sleepMillis = Long.max(tick.sleep().toMillis(), 0);
        log.info("Idle, waiting {}ms", sleepMillis);
        Thread.sleep(sleepMillis);
      } else {
        break;
      }
    }

    final TaskId rootTaskId = withRocksDB(dbPath, db -> read(db, ROOT_TASK_ID));
    final EvaluatingContext evaluatingContext = evaluationContext(persistedTasksDir);
    final String result = evaluatingContext.readExistingOutput(rootTaskId);

    log.info("workflow result: {}", result);

    assertThat(result, is("foo bar foo bar quux deadbeef"));
  }

  public static EvaluatingContext evaluationContext(String persistedTasksDir) {
    return new EvaluatingContext(Paths.get(persistedTasksDir), EvalContext.sync());
  }

  public static void persistWorkflow(String dbPath, String dir) {
    final Task<String> root = workflow(Date.of(LocalDate.now()));

    final PersistingContext persistingContext = new PersistingContext(
        Paths.get(dir), EvalContext.sync());

    try {
      MemoizingContext.composeWith(persistingContext)
          .evaluate(root).toFuture().exceptionally(t -> "").get();
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException(e);
    }

    final Map<TaskId, Path> files = persistingContext.getFiles();

    withRocksDB(dbPath, db -> {
      for (Task<?> task : tasks(root)) {
        write(db, TASK_PATH, task.id().toString(), files.get(task.id()).toAbsolutePath().toString());
        write(db, TASK, task.id().toString(), "");
      }
      write(db, ROOT_TASK_ID, root.id());
      return null;
    });
  }

  static Task<String> workflow(Date date) {

    final Task<String> foo = Task.named("foo")
        .ofType(String.class)
        .process(() -> {
          log.info("process fn: foo");
          return "foo";
        });

    final Task<String> bar = Task.named("bar")
        .ofType(String.class)
        .process(() -> {
          log.info("process fn: bar");
          return "bar";
        });

    final Task<String> quux = Task.named("quux")
        .ofType(String.class)
        .output(new DoneOutput<>("quux"))
        .process(s -> {
          throw new AssertionError("execution of quux not expected");
        });

    final Task<String> deadbeef = Task.named("deadbeef")
        .ofType(String.class)
        .output(new DoneOutput<>("deadbeef"))
        .operator(Jobs.JobOperator.create())
        .process((s, j) -> {
          throw new AssertionError("execution of deadbeef not expected");
        });

    final Task<String> baz = Task.named("baz")
        .ofType(String.class)
        .operator(Jobs.JobOperator.create())
        .input(() -> foo)
        .input(() -> bar)
        .process((spec, a, b) -> {
          log.info("process fn: baz");
          return spec
              .options(() -> ImmutableMap.of("foo", "bar"))
              .pipeline(ctx -> ctx
                  .readFrom("foo")
                  .map("_ + _")
                  .writeTo("bar"))
              .validation(r -> {
                if (r.records == 0) {
                  throw new AssertionError("no records seen!");
                }
              })
              .success(r -> a + " " + b);
        });

    return Task.named("root")
        .ofType(String.class)
        .operator(Jobs.JobOperator.create())
        .input(() -> foo)
        .input(() -> bar)
        .input(() -> baz)
        .input(() -> quux)
        .input(() -> deadbeef)
        .process((spec, a, b, c, d, e) -> {
          log.info("process fn: main: + " + date);
          return spec
              .options(() -> ImmutableMap.of("foo", "bar"))
              .pipeline(ctx -> ctx
                  .readFrom("foo")
                  .map("_ + _")
                  .writeTo("bar"))
              .validation(r -> {
                if (r.records == 0) {
                  throw new AssertionError("no records seen!");
                }
              })
              .success(r -> String.join(" ", a, b, c, d, e));
        });
  }

  private static Tick runWorkflowTick(String dbPath, String persistedTasksDir) {
    return withRocksDB(dbPath, db -> {
      try {
        return runWorkflowTick0(db, persistedTasksDir);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    });
  }

  private static Tick runWorkflowTick0(RocksDB db, String persistedTasksDir) throws RocksDBException, IOException {

    boolean progressed = false;

    // Evaluate tasks
    final List<String> tasks = readKeys(db, TASK);
    log.info("# tasks={}", tasks.size());
    for (String taskId : tasks) {
      final String taskPath = read(db, TASK_PATH, taskId);
      final Task<?> task = deserialize(Paths.get(taskPath));
      final boolean inputReady = task.inputs().stream().map(Task::id)
          .allMatch(evaluationContext(persistedTasksDir)::isEvaluated);
      if (!inputReady) {
        log.info("task {}: inputs not yet ready", task.id());
        continue;
      }

      progressed = true;

      // Check if output has already been produced and execution can be skipped
      @SuppressWarnings("unchecked") final Optional<TaskOutput<?, Object>> outputContext =
          (Optional) TaskOutput.output(task);
      if (outputContext.isPresent()) {
        @SuppressWarnings("unchecked") final Optional<Object> lookup = outputContext.get()
            .lookup((Task<Object>) task);
        if (lookup.isPresent()) {
          log.info("task {}: output already exists, not executing", task.id());
          evaluationContext(persistedTasksDir).persist(task.id(), lookup.get());
          delete(db, TASK, taskId);
          continue;
        }
      }

      // Execute task
      if (OperationExtractingContext.operator(task).isPresent()) {
        // Start operation
        log.info("task {}: extracting operation", task.id());
        final Operation op = fork(() ->
            OperationExtractingContext.extract(task, taskOutput(evaluationContext(persistedTasksDir))));
        final TaskOperator.Operation operation = op.operator.start(op.spec, Listener.NOP);
        log.info("task {}: operation {}: starting", task.id(), operation);
        write(db, OPERATION, task.id().toString(), operation);
      } else {
        // Execute generic task
        log.info("task {}: executing generic task", task.id());
        fork(() -> {
          try {
            return evaluationContext(persistedTasksDir).evaluateTaskFrom(Paths.get(taskPath)).toFuture().get();
          } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
          }
        });
        log.info("task {}: generic task completed", task.id());
      }

      delete(db, TASK, taskId);
    }

    // Run operators
    final List<String> operations = readKeys(db, OPERATION);
    log.info("# operations={}", operations.size());
    Instant firstPoll = null;
    for (String taskId : operations) {

      final Instant pollDeadline = read(db, OPERATION_SCHEDULE, taskId, Instant.MIN);
      if (firstPoll == null || firstPoll.isAfter(pollDeadline)) {
        firstPoll = pollDeadline;
      }

      // Due for polling?
      if (Instant.now().isBefore(pollDeadline)) {
        continue;
      }

      // Poll!
      final TaskOperator.Operation operation = read(db, OPERATION, taskId, ASSERTION_ERROR);
      log.info("task {}: operation {}: polling", taskId, operation);
      Optional<Object> state = Optional.ofNullable(read(db, OPERATION_STATE, taskId));
      final Result result = operation.perform(state, Listener.NOP);

      // Done?
      if (!result.isDone()) {
        log.info("task {}: operation {}: not done, checking again in {}", taskId, operation, result.pollInterval());
        write(db, OPERATION_SCHEDULE, taskId, Instant.now().plus(result.pollInterval()));
        if (result.state().isPresent()) {
          write(db, OPERATION_STATE, taskId, result.state().get());
        }
        continue;
      }

      // Done!
      log.info("task {}: operation {}: completed", taskId, operation);
      progressed = true;

      if (result.isSuccess()) {
        evaluationContext(persistedTasksDir).persist(TaskId.parse(taskId), result.output());
      } else {
        throw new RuntimeException(result.cause());
      }

      delete(db, OPERATION_STATE, taskId);
      delete(db, OPERATION_SCHEDULE, taskId);
      delete(db, OPERATION, taskId);
    }

    // Sanity check
    {
      final Set<String> operationKeys = ImmutableSet.copyOf(readKeys(db, OPERATION));
      assertThat(ImmutableSet.copyOf(readKeys(db, OPERATION_SCHEDULE)), is(operationKeys));
      for (String taskId : readKeys(db, OPERATION_STATE)) {
        assertThat(operationKeys.contains(taskId), is(true));
      }
    }

    // Any tasks or operations left?
    if (count(db, TASK) == 0 && count(db, OPERATION) == 0) {
      log.info("all tasks and operations completed");
      return Tick.done();
    }

    if (progressed) {
      return Tick.continuing(Duration.ofSeconds(0));
    }

    return Tick.continuing(Duration.between(Instant.now(), firstPoll));
  }

  private static <T> T fork(Fn<T> f) {
    if (DEBUG) {
      return f.get();
    }
    try (ForkingExecutor executor = new ForkingExecutor()) {
      return executor.execute(f);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private static void write(RocksDB db, String key, Object value) throws RocksDBException {
    write(db, "", key, value);
    db.put(key.getBytes(UTF_8), serialize(value));
  }

  private static void write(RocksDB db, String type, String key, Object value) throws RocksDBException {
    db.put(key(type, key), serialize(value));
  }

  private static <T> T read(RocksDB db, String key) throws RocksDBException {
    return read(db, "", key, (t, k) -> null);
  }

  private static <T> T read(RocksDB db, String type, String key) throws RocksDBException {
    return read(db, type, key, (t, k) -> null);
  }

  private static void delete(RocksDB db, String type, String key) throws RocksDBException {
    db.delete(key(type, key));
  }

  private static List<String> readKeys(RocksDB db, String type) {
    final List<String> keys = new ArrayList<>();
    try (final RocksIterator iterator = db.newIterator()) {
      final byte[] prefix = key(type, "");
      iterator.seek(prefix);
      while (iterator.isValid()) {
        final byte[] rawKey = iterator.key();
        if (!hasPrefix(rawKey, prefix)) {
          break;
        }
        keys.add(new String(rawKey, prefix.length, rawKey.length - prefix.length, UTF_8));
        iterator.next();
      }
      return keys;
    }
  }

  private static long count(RocksDB db, String type) {
    long n = 0;
    try (final RocksIterator iterator = db.newIterator()) {
      final byte[] prefix = key(type, "");
      iterator.seek(prefix);
      while (iterator.isValid()) {
        final byte[] rawKey = iterator.key();
        if (!hasPrefix(rawKey, prefix)) {
          break;
        }
        n++;
        iterator.next();
      }
      return n;
    }
  }

  private static boolean hasPrefix(byte[] key, byte[] prefix) {
    if (key.length < prefix.length) {
      return false;
    }
    for (int i = 0; i < prefix.length; i++) {
      if (key[i] != prefix[i]) {
        return false;
      }
    }
    return true;
  }

  private static <T> T read(RocksDB db, String type, String key, T defaultValue) throws RocksDBException {
    return read(db, type, key, (t, k) -> defaultValue);
  }

  private static <T> T read(RocksDB db, String type, String key, F2<String, String, T> defaultValue)
      throws RocksDBException {
    final byte[] bytes = db.get(key(type, key));
    if (bytes == null) {
      return defaultValue.apply(type, key);
    }
    return deserialize(bytes);
  }

  private static byte[] key(String type, String key) {
    if (type.indexOf('|') != -1) {
      throw new IllegalArgumentException();
    }
    if (key.indexOf('|') != -1) {
      throw new IllegalArgumentException();
    }
    return (type + '|' + key).getBytes(UTF_8);
  }

  private static <T> T withRocksDB(final String dbPath, RocksDBFn<T> f) {
    RocksDB.loadLibrary();
    try (final Options options = new Options().setCreateIfMissing(true)) {
      try (final RocksDB db = RocksDB.open(options, dbPath)) {
        try {
          return f.get(db);
        } catch (RocksDBException e) {
          throw new RuntimeException(e);
        }
      }
    } catch (RocksDBException e) {
      throw new RuntimeException(e);
    }
  }

  private static F1<TaskId, ?> taskOutput(EvaluatingContext evaluatingContext) {
    return taskId -> {
      try {
        return evaluatingContext.readExistingOutput(taskId);
      } catch (IOException ex) {
        throw new RuntimeException(ex);
      }
    };
  }

  private static void tasks(Task<?> task, Consumer<Task<?>> f) {
    f.accept(task);
    task.inputs().forEach(upstream -> tasks(upstream, f));
  }

  private static Set<Task<?>> tasks(Task<?> task) {
    final Set<Task<?>> tasks = new HashSet<>();
    tasks(task, tasks::add);
    return tasks;
  }

  private static class DoneOutput<T> extends TaskOutput<String, T> {

    private final T value;

    DoneOutput(T value) {
      this.value = value;
    }

    @Override
    public String provide(EvalContext evalContext) {
      return "";
    }

    @Override
    public Optional<T> lookup(Task<T> task) {
      return Optional.of(value);
    }
  }

  private interface RocksDBFn<T> {

    T get(RocksDB db) throws RocksDBException;
  }

  @AutoValue
  static abstract class Tick implements Serializable {

    abstract Duration sleep();

    abstract boolean isDone();

    static Tick continuing(Duration sleep) {
      return new AutoValue_EphemeralExecutionTest_Tick(sleep, false);
    }

    static Tick done() {
      return new AutoValue_EphemeralExecutionTest_Tick(Duration.ZERO, true);
    }
  }
}
