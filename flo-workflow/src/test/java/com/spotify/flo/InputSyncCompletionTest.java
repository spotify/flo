/*-
 * -\-\-
 * Flo Workflow Definition
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

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.Test;

public class InputSyncCompletionTest {

  EvalContext context = EvalContext.async(Executors.newFixedThreadPool(2));

  CountDownLatch blocked = new CountDownLatch(1);
  Task<Integer> blocking = Task.named("blocking").ofType(Integer.class)
      .process(() -> {
        try {
          blocked.await(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
        return 42;
      });

  private Task<Integer> failing(String msg) {
    return Task.named("failing").ofType(Integer.class)
      .process(() -> {
        throw new RuntimeException(msg);
      });
  }

  @Test
  public void shouldNotCompleteTaskBeforeAllInputsAreDone_FailingBefore() throws Exception {
    Task<String> task = Task.named("sync").ofType(String.class)
        .input(() -> failing("failed"))
        .input(() -> blocking)
        .process((a, b) -> "should not happen");

    awaitBlocked(task);
  }

  @Test
  public void shouldNotCompleteTaskBeforeAllInputsAreDone_FailingAfter() throws Exception {
    Task<String> task = Task.named("sync").ofType(String.class)
        .input(() -> blocking)
        .input(() -> failing("failed"))
        .process((a, b) -> "should not happen");

    awaitBlocked(task);
  }

  @Test
  public void shouldNotCompleteTaskBeforeAllInputsAreDone_AsInputListBefore() throws Exception {
    Task<String> task = Task.named("sync").ofType(String.class)
        .inputs(() -> Arrays.asList(failing("failed"), blocking))
        .process(a -> "should not happen");

    awaitBlocked(task);
  }

  @Test
  public void shouldNotCompleteTaskBeforeAllInputsAreDone_AsInputListAfter() throws Exception {
    Task<String> task = Task.named("sync").ofType(String.class)
        .inputs(() -> Arrays.asList(blocking, failing("failed")))
        .process(a -> "should not happen");

    awaitBlocked(task);
  }

  @Test
  public void shouldPropagateExceptionsAsSuppressed() throws Exception {
    Task<String> task = Task.named("failing").ofType(String.class)
        .input(() -> failing("failed 1"))
        .input(() -> failing("failed 2"))
        .process((a, b) -> "should not happen");

    AwaitValue<Throwable> eval = new AwaitValue<>();
    context.evaluate(task).onFail(eval);

    Throwable throwable = eval.awaitAndGet();

    assertThat(throwable, instanceOf(RuntimeException.class));
    assertThat(throwable.getMessage(), is("failed 1"));
    assertThat(throwable.getSuppressed().length, is(1));

    throwable = throwable.getSuppressed()[0];
    assertThat(throwable.getMessage(), is("failed 2"));
  }

  @Test
  public void shouldPropagateExceptionsAsSuppressedFromMultipleLevels() throws Exception {
    Task<String> taskLevel2 = Task.named("failing-2").ofType(String.class)
        .input(() -> failing("failed 1"))
        .input(() -> failing("failed 2"))
        .process((a, b) -> "should not happen");

    Task<String> task = Task.named("failing-1").ofType(String.class)
        .input(() -> taskLevel2)
        .input(() -> failing("failed 3"))
        .process((a, b) -> "should not happen");

    AwaitValue<Throwable> eval = new AwaitValue<>();
    context.evaluate(task).onFail(eval);

    Throwable throwable = eval.awaitAndGet();

    assertThat(throwable, instanceOf(RuntimeException.class));
    assertThat(throwable.getMessage(), is("failed 1"));
    assertThat(throwable.getSuppressed().length, is(2));
    assertThat(throwable.getSuppressed()[0].getMessage(), is("failed 2"));
    assertThat(throwable.getSuppressed()[1].getMessage(), is("failed 3"));
  }

  private void awaitBlocked(Task<String> task) throws InterruptedException {
    AwaitValue<Throwable> eval = new AwaitValue<>();
    context.evaluate(task).onFail(eval);

    // should not complete before we unblock the blocked promise
    assertThat(eval.await(1, TimeUnit.SECONDS), is(false));

    blocked.countDown();
    Throwable throwable = eval.awaitAndGet();

    assertThat(throwable, instanceOf(RuntimeException.class));
    assertThat(throwable.getMessage(), is("failed"));
  }
}
