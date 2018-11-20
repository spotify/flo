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

import static com.spotify.flo.context.FloRunner.runTask;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.argThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import com.google.common.collect.ImmutableMap;
import com.spotify.flo.FloTesting;
import com.spotify.flo.Serialization;
import com.spotify.flo.Task;
import com.spotify.flo.TaskId;
import com.spotify.flo.TestScope;
import com.spotify.flo.Tracing;
import com.spotify.flo.context.FloRunner.Result;
import com.spotify.flo.context.InstrumentedContext.Listener.Phase;
import com.spotify.flo.context.Jobs.JobOperator;
import com.spotify.flo.context.Mocks.DataProcessing;
import com.spotify.flo.context.Mocks.PublishingOutput;
import com.spotify.flo.context.Mocks.StorageLookup;
import com.spotify.flo.freezer.Persisted;
import com.spotify.flo.status.NotReady;
import com.spotify.flo.status.NotRetriable;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.lang.management.ManagementFactory;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RunWith(MockitoJUnitRunner.class)
public class FloRunnerTest {

  private static final Logger log = LoggerFactory.getLogger(FloRunnerTest.class);

  static volatile String listenerOutputDir;

  private final Task<String> FOO_TASK = Task.named("task").ofType(String.class)
      .process(() -> "foo");

  private TerminationHook validTerminationHook;
  private TerminationHook exceptionalTerminationHook;

  @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Mock InstrumentedContext.Listener listener;

  @Before
  public void setUp() throws IOException {
    listenerOutputDir = temporaryFolder.newFolder().getAbsolutePath();

    exceptionalTerminationHook = mock(TerminationHook.class);
    doThrow(new RuntimeException("hook exception")).when(exceptionalTerminationHook).accept(any());

    validTerminationHook = mock(TerminationHook.class);
    doNothing().when(validTerminationHook).accept(any());

    TestTerminationHookFactory.injectCreator((config) -> validTerminationHook);
  }

  @Test
  public void nonBlockingRunnerDoesNotBlock() throws Exception {
    final Path directory = temporaryFolder.newFolder().toPath();
    final File startedFile = directory.resolve("started").toFile();
    final File latchFile = directory.resolve("latch").toFile();
    final File happenedFile = directory.resolve("happened").toFile();

    final Task<Void> task = Task.named("task").ofType(Void.class)
        .process(() -> {
          try {
            startedFile.createNewFile();
            while (true) {
              if (latchFile.exists()) {
                happenedFile.createNewFile();
                return null;
              }
              Thread.sleep(100);
            }
          } catch (IOException | InterruptedException e) {
            throw new RuntimeException(e);
          }
        });

    final Result<Void> result = runTask(task);

    // Verify that the task ran at all
    CompletableFuture.supplyAsync(() -> {
      while (true) {
        if (startedFile.exists()) {
          return true;
        }
        try {
          Thread.sleep(100);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      }
    }).get(30, SECONDS);

    // Wait a little more to ensure that the task process has some time to write the "happened" file
    try {
      result.future().get(2, SECONDS);
      fail();
    } catch (TimeoutException ignore) {
    }

    // If this file doesn't exist now, it's likely that runTask doesn't block
    assertThat(happenedFile.exists(), is(false));

    latchFile.createNewFile();
  }

  @Test
  public void blockingRunnerBlocks() throws IOException {
    final File file = temporaryFolder.newFile();

    final Task<Void> task = Task.named("task").ofType(Void.class)
        .process(() -> {
          try {
            Thread.sleep(10);
            try {
              Files.write(file.toPath(), "hello".getBytes(UTF_8));
            } catch (IOException e) {
              throw new RuntimeException(e);
            }
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
          return null;
        });

    runTask(task).waitAndExit(status -> { });

    assertThat(new String(Files.readAllBytes(file.toPath()), UTF_8), is("hello"));
  }

  @Test
  public void valueIsPassedInFuture() throws Exception {
    final String result = runTask(FOO_TASK).future().get(30, SECONDS);

    assertThat(result, is("foo"));
  }

  @Test
  public void testSerializeException() throws Exception {
    final File file = temporaryFolder.newFile();
    file.delete();
    Serialization.serialize(new RuntimeException("foo"), file.toPath());
  }

  @Test
  public void exceptionsArePassed() throws Exception {
    final Task<String> task = Task.named("foo").ofType(String.class)
        .process(() -> {
          throw new RuntimeException("foo");
        });

    Throwable exception = null;
    try {
      runTask(task).value();
    } catch (ExecutionException e) {
      exception = e.getCause();
    }
    assertThat(exception, is(instanceOf(RuntimeException.class)));
    assertThat(exception.getMessage(), is("foo"));
  }

  @Test
  public void errorsArePassed() throws Exception {
    final Task<String> task = Task.named("foo").ofType(String.class)
        .process(() -> {
          throw new Error("foo");
        });

    Throwable exception = null;
    try {
      runTask(task).value();
    } catch (ExecutionException e) {
      exception = e.getCause();
    }
    assertThat(exception, is(instanceOf(Error.class)));
    assertThat(exception.getMessage(), is("foo"));
  }

  @Test
  public void persistedExitsZero() {
    final Task<Void> task = Task.named("persisted").ofType(Void.class)
        .process(() -> {
          throw new Persisted();
        });

    AtomicInteger status = new AtomicInteger(1);

    runTask(task).waitAndExit(status::set);

    assertThat(status.get(), is(0));
  }

  @Test
  public void valuesCanBeWaitedOn() throws Exception {
    final String result = runTask(FOO_TASK).value();

    assertThat(result, is("foo"));
  }

  @Test
  public void notReadyExitsTwenty() {
    final Task<String> task = Task.named("task").ofType(String.class)
        .process(() -> {
          throw new NotReady();
        });

    AtomicInteger status = new AtomicInteger();

    runTask(task).waitAndExit(status::set);

    assertThat(status.get(), is(20));
  }

  @Test
  public void notRetriableExitsFifty() {
    final Task<String> task = Task.named("task").ofType(String.class)
        .process(() -> {
          throw new NotRetriable();
        });

    AtomicInteger status = new AtomicInteger();

    runTask(task).waitAndExit(status::set);

    assertThat(status.get(), is(50));
  }

  @Test
  public void exceptionsExitNonZero() {
    final Task<String> task = Task.named("task").ofType(String.class)
        .process(() -> {
          throw new RuntimeException("this task should throw");
        });

    AtomicInteger status = new AtomicInteger();

    runTask(task).waitAndExit(status::set);

    assertThat(status.get(), is(1));
  }

  @Test
  public void ignoreExceptionsFromTerminationHook() {
    TestTerminationHookFactory.injectHook(exceptionalTerminationHook);

    AtomicInteger status = new AtomicInteger();
    runTask(FOO_TASK).waitAndExit(status::set);

    verify(exceptionalTerminationHook, times(1)).accept(eq(0));
    assertThat(status.get(), is(0));
  }

  @Test
  public void validateTerminationHookInvocationOnTaskSuccess() {
    TestTerminationHookFactory.injectHook(validTerminationHook);

    AtomicInteger status = new AtomicInteger();
    runTask(FOO_TASK).waitAndExit(status::set);

    verify(validTerminationHook, times(1)).accept(eq(0));
    assertThat(status.get(), is(0));
  }

  @Test
  public void validateTerminationHookInvocationOnTaskFailure() {
    final Task<String> task = Task.named("task").ofType(String.class)
        .process(() -> {
          throw new RuntimeException("this task should throw");
        });

    TestTerminationHookFactory.injectHook(validTerminationHook);

    AtomicInteger status = new AtomicInteger();
    runTask(task).waitAndExit(status::set);

    verify(validTerminationHook, times(1)).accept(eq(1));
    assertThat(status.get(), is(1));
  }

  @Test(expected = RuntimeException.class)
  public void failOnExceptionalTerminationHookFactory() {
    TestTerminationHookFactory.injectCreator((config) -> {
      throw new RuntimeException("factory exception");
    });
    runTask(FOO_TASK);
  }

  @Test
  public void taskIdIsInContext() throws Exception {
    final Task<TaskId> task = Task.named("task").ofType(TaskId.class)
        .process(() -> Tracing.currentTaskId());

    final Result<TaskId> result = runTask(task);

    assertThat(result.value(), is(task.id()));
  }

  @Test
  public void tasksRunInProcesses() throws Exception {

    final Instant today = Instant.now().truncatedTo(ChronoUnit.DAYS);
    final Instant yesterday = today.minus(1, ChronoUnit.DAYS);

    final Task<String> baz = Task.named("baz", today).ofType(String.class)
        .process(() -> {
          final String bazJvm = jvmName();
          log.info("baz: bazJvm={}, today={}", bazJvm, today);
          return bazJvm;
        });

    final Task<String[]> foo = Task.named("foo", yesterday).ofType(String[].class)
        .input(() -> baz)
        .process(bazJvm -> {
          final String fooJvm = jvmName();
          log.info("foo: fooJvm={}, bazJvm={}, yesterday={}", fooJvm, bazJvm, yesterday);
          return new String[]{bazJvm, fooJvm};
        });

    final Task<String> quux = Task.named("quux", today).ofType(String.class)
        .process(() -> {
          final String quuxJvm = jvmName();
          log.info("quux: quuxJvm={}, yesterday={}", quuxJvm, yesterday);
          return quuxJvm;
        });

    final Task<String[]> bar = Task.named("bar", today, yesterday).ofType(String[].class)
        .input(() -> foo)
        .input(() -> quux)
        .process((bazFooJvms, quuxJvm) -> {
          final String barJvm = jvmName();
          log.info("bar: barJvm={}, bazFooJvms={}, quuxJvm={} today={}, yesterday={}",
              barJvm, bazFooJvms, quuxJvm, today, yesterday);
          return Stream.concat(
              Stream.of(barJvm),
              Stream.concat(
                  Stream.of(bazFooJvms),
                  Stream.of(quuxJvm))
          ).toArray(String[]::new);
        });

    final List<String> jvms = Arrays.asList(runTask(bar).value());

    final String mainJvm = jvmName();

    log.info("main jvm: {}", mainJvm);
    log.info("task jvms: {}", jvms);
    final Set<String> uniqueJvms = new HashSet<>(jvms);
    assertThat(uniqueJvms.size(), is(4));
    assertThat(uniqueJvms, not(contains(mainJvm)));
  }

  @Test
  public void isTestShouldBeTrueInTestScope() throws Exception {
    assertThat(FloTesting.isTest(), is(false));
    try (TestScope ts = FloTesting.scope()) {
      assertThat(FloTesting.isTest(), is(true));
      final Task<Boolean> isTest = Task.named("task").ofType(Boolean.class)
          .process(FloTesting::isTest);
      assertThat(runTask(isTest).future().get(30, SECONDS), is(true));
    }
    assertThat(FloTesting.isTest(), is(false));
  }

  @Test
  public void mockingInputsOutputsAndContextShouldBePossibleInTestScope() throws Exception {

    // Mock input, data processing results and verify lookups and publishing after the fact
    try (TestScope ts = FloTesting.scope()) {
      final URI barInput = URI.create("gs://bar/4711/");
      final String jobResult = "42";
      final URI publishResult = URI.create("meta://bar/4711/");

      PublishingOutput.mock().publish(jobResult, publishResult);
      StorageLookup.mock().data("bar", barInput);
      DataProcessing.mock().result("quux.baz", barInput, jobResult);

      final Task<String> task = Task.named("task").ofType(String.class)
          .input(() -> StorageLookup.of("bar"))
          .output(PublishingOutput.of("foo"))
          .process((bar, publisher) -> {
            // Run a data processing job and publish the result
            final String result = DataProcessing.runJob("quux.baz", bar);
            return publisher.publish(result);
          });

      assertThat(runTask(task).future().get(30, SECONDS), is(jobResult));

      assertThat(DataProcessing.mock().jobRuns("quux.baz", barInput), is(1));
      assertThat(StorageLookup.mock().lookups("bar"), is(1));
      assertThat(PublishingOutput.mock().lookups("foo"), is(1));
      assertThat(PublishingOutput.mock().published("foo"), contains(jobResult));
    }

    // Verify that all mocks are cleared when leaving scope
    try (TestScope ts = FloTesting.scope()) {
      assertThat(PublishingOutput.mock().published("foo"), is(empty()));
      assertThat(PublishingOutput.mock().lookups("foo"), is(0));
    }
  }

  @Test
  public void mockingContextExistsShouldMakeProcessFnNotRunInTestScope() throws Exception {

    // Mock a context lookup and verify that the process fn does not run
    try (TestScope ts = FloTesting.scope()) {
      PublishingOutput.mock().value("foo", "17");

      final Task<String> task = Task.named("task").ofType(String.class)
          .output(PublishingOutput.of("foo"))
          .process(v -> { throw new AssertionError(); });

      assertThat(runTask(task).future().get(30, SECONDS), is("17"));

      assertThat(PublishingOutput.mock().lookups("foo"), is(1));
      assertThat(PublishingOutput.mock().published("foo"), is(empty()));
    }
  }

  @Test
  public void shouldDryForkInTestMode() throws Exception {
    final String mainJvmName = jvmName();
    final Rock rock = new Rock();

    final Task<Rock> task = Task.named("task").ofType(Rock.class)
        .process(() -> rock);

    final Rock result;

    try (TestScope ts = FloTesting.scope()) {
      result = FloRunner.runTask(task).future().get(30, SECONDS);
    }

    // Check that the identity of the value changed due to serialization (dry fork)
    assertThat(result, is(not(rock)));

    // Check that the process fn ran in the main jvm
    assertThat(result.jvmName, is(mainJvmName));
  }

  @Test
  public void testOperator() throws Exception {
    final String mainJvm = jvmName();
    final Instant today = Instant.now().truncatedTo(ChronoUnit.DAYS);
    final Task<JobResult> task = Task.named("task", today).ofType(JobResult.class)
        .operator(JobOperator.create())
        .process(job -> job
            .options(() -> ImmutableMap.of("quux", 17))
            .pipeline(ctx -> ctx.readFrom("foo").map("x + y").writeTo("baz"))
            .validation(result -> {
              if (result.records < 5) {
                throw new AssertionError("Too few records seen!");
              }
            })
            .success(result -> new JobResult(jvmName(), "hdfs://foo/bar")));

    final JobResult result = FloRunner.runTask(task)
        .future().get(30, SECONDS);

    assertThat(result.jvmName, is(not(mainJvm)));
    assertThat(result.uri, is("hdfs://foo/bar"));
  }

  @Test
  public void tasksAreObservedByInstrumentedContext() throws Exception {
    final Task<String> fooTask = Task.named("foo").ofType(String.class)
        .process(() -> "foo");

    final Task<String> barTask = Task.named("bar").ofType(String.class)
        .operator(JobOperator.create())
        .input(() -> fooTask)
        .process((op, bar) -> op.success(res -> "foo" + bar));

    FloRunner.runTask(barTask).future().get(30, SECONDS);

    RecordingListener.replay(listener);

    verify(listener).task(argThat(task -> fooTask.id().equals(task.id())));
    verify(listener).task(argThat(task -> barTask.id().equals(task.id())));
    verify(listener).status(fooTask.id(), Phase.START);
    verify(listener).status(fooTask.id(), Phase.SUCCESS);
    verify(listener).status(barTask.id(), Phase.START);
    verify(listener).status(barTask.id(), Phase.SUCCESS);
    verify(listener).meta(barTask.id(), Collections.singletonMap("task-id", barTask.id().toString()));
    verifyNoMoreInteractions(listener);
  }

  @Test
  public void evaluatesTaskOnce() throws Exception {
    final String fooRuns = temporaryFolder.newFolder().toString();
    Task<String> foo = Task.named("foo").ofType(String.class)
        .process(() -> {
          final Path marker = Paths.get(fooRuns, UUID.randomUUID().toString());
          try {
            Files.createFile(marker);
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
          return marker.toString();
        });

    Task<String> bar = Task.named("bar").ofType(String.class)
        .input(() -> foo)
        .input(() -> foo)
        .process((foo1, foo2) -> foo1 + foo2);

    FloRunner.runTask(bar).future().get();

    assertThat(Files.list(Paths.get(fooRuns)).count(), is(1L));
  }

  private static String jvmName() {
    return ManagementFactory.getRuntimeMXBean().getName();
  }

  private static class JobResult implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String jvmName;
    private final String uri;

    JobResult(String jvmName, String uri) {
      this.jvmName = jvmName;
      this.uri = uri;
    }
  }

  private static class Rock implements Serializable {

    private static final long serialVersionUID = 1L;

    final String jvmName = jvmName();
  }
}
