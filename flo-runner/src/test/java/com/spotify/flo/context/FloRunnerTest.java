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
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.isOneOf;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import com.spotify.flo.Task;
import com.spotify.flo.freezer.Persisted;
import com.spotify.flo.status.NotReady;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.junit.Test;

public class FloRunnerTest {

  @Test
  public void nonBlockingRunnerDoesNotBlock() {
    final CompletableFuture<String> future = new CompletableFuture<>();
    final Task<Void> task = Task.named("task").ofType(Void.class)
        .process(() -> {
          try {
            Thread.sleep(10);
            future.complete("foo");
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
          return null;
        });

    runTask(task);

    assertThat(future.isDone(), is(false));
  }

  @Test
  public void blockingRunnerBlocks() {
    final CompletableFuture<String> future = new CompletableFuture<>();
    final Task<Void> task = Task.named("task").ofType(Void.class)
        .process(() -> {
          try {
            Thread.sleep(10);
            future.complete("foo");
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
          return null;
        });

    runTask(task).waitAndExit(status -> { });

    assertThat(future.isDone(), is(true));
  }

  @Test
  public void valueIsPassedInFuture() throws Exception {
    final Task<String> task = Task.named("task").ofType(String.class)
        .process(() -> "foo");

    final String result = runTask(task).future().get(1, TimeUnit.SECONDS);

    assertThat(result, is("foo"));
  }

  @Test
  public void valuesArePassedToDependents() throws Exception {
    final Task<String> bar = Task.named("bar").ofType(String.class)
        .process(() -> "bar");

    final CompletableFuture<String> future = new CompletableFuture<>();

    final Task<Boolean> foo = Task.named("foo").ofType(Boolean.class)
        .in(() -> bar)
        .process(future::complete);

    runTask(foo).future().get(1, TimeUnit.SECONDS);

    assertThat(future.get(), is("bar"));
  }

  @Test
  public void exceptionsThrownInParallelAreUndeterministic() throws Exception {
    // TODO: suppress exceptions instead
    final RuntimeException e1 = new RuntimeException("1");
    final RuntimeException e2 = new RuntimeException("2");

    final Task<String> t1 = Task.named("t1").ofType(String.class)
        .process(() -> {
          throw e1;
        });

    final Task<String> t2 = Task.named("t2").ofType(String.class)
        .process(() -> {
          throw e2;
        });

    final Task<String> task = Task.named("task").ofType(String.class)
        .in(() -> t1)
        .in(() -> t2)
        .process((v1, v2) -> null);

    try {
      runTask(task).value();
      fail();
    } catch (ExecutionException e) {
      assertThat(e.getCause(), isOneOf(e1, e2));
    }
  }

  @Test
  public void exceptionsArePassed() throws Exception {
    final RuntimeException expectedException = new RuntimeException("foo");

    final Task<String> task = Task.named("foo").ofType(String.class)
        .process(() -> {
          throw expectedException;
        });

    try {
      runTask(task).value();
      fail();
    } catch (ExecutionException e) {
      assertThat(e.getCause(), is(expectedException));
    }
  }

  @Test
  public void exceptionsArePassedThroughDag() throws Exception {
    final RuntimeException expectedException = new RuntimeException("foo");

    final Task<String> bar = Task.named("bar").ofType(String.class)
        .process(() -> {
          throw expectedException;
        });

    final Task<Void> foo = Task.named("foo").ofType(Void.class)
        .in(() -> bar)
        .process(b -> null);

    try {
      runTask(foo).value();
      fail();
    } catch (ExecutionException e) {
      assertThat(e.getCause(), is(expectedException));
    }
  }

  @Test
  public void persistedExitsZero() throws Exception {
    final Task<Void> task = Task.named("persisted").ofType(Void.class)
        .process(() -> {
          throw new Persisted();
        });

    final CompletableFuture<Integer> future = new CompletableFuture<>();

    runTask(task).waitAndExit(future::complete);

    final int status = future.get(1, TimeUnit.SECONDS);

    assertThat(status, is(0));
  }

  @Test
  public void valuesCanBeWaitedOn() throws Exception {
    final Task<String> task = Task.named("task").ofType(String.class)
        .process(() -> "foo");

    final String result = runTask(task).value();

    assertThat(result, is("foo"));
  }

  @Test
  public void notReadyExitsTwenty() throws Exception {
    final Task<String> task = Task.named("task").ofType(String.class)
        .process(() -> {
          throw new NotReady();
        });

    final CompletableFuture<Integer> future = new CompletableFuture<>();

    runTask(task).waitAndExit(future::complete);

    final int status = future.get(1, TimeUnit.SECONDS);

    assertThat(status, is(20));
  }

  @Test
  public void exceptionsExitNonZero() throws Exception {
    final Task<String> throwingTask = Task.named("throwingTask").ofType(String.class)
        .process(() -> {
          throw new RuntimeException("this task should throw");
        });

    final CompletableFuture<Integer> future = new CompletableFuture<>();

    runTask(throwingTask).waitAndExit(future::complete);

    final int status = future.get(1, TimeUnit.SECONDS);

    assertThat(status, is(1));
  }
}
