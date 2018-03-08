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
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.Test;

public class InputSyncCompletionTest {

  EvalContext context = EvalContext.async(Executors.newFixedThreadPool(2));

  AwaitValue<EvalContext.Promise<Integer>> blocked = new AwaitValue<>();
  Task<Integer> blocking = Task.named("blocking").ofType(Integer.class)
      .processWithContext((ec) -> {
        EvalContext.Promise<Integer> promise = ec.promise();
        blocked.accept(promise);
        return promise.value();
      });

  Task<Integer> failing = Task.named("failing").ofType(Integer.class)
      .process(() -> {
        throw new RuntimeException("failed");
      });

  @Test
  public void shouldNotCompleteTaskBeforeAllInputsAreDone_FailingBefore() throws Exception {
    Task<String> task = Task.named("sync").ofType(String.class)
        .input(() -> failing)
        .input(() -> blocking)
        .process((a, b) -> "should not happen");

    awaitBlocked(task);
  }

  @Test
  public void shouldNotCompleteTaskBeforeAllInputsAreDone_FailingAfter() throws Exception {
    Task<String> task = Task.named("sync").ofType(String.class)
        .input(() -> blocking)
        .input(() -> failing)
        .process((a, b) -> "should not happen");

    awaitBlocked(task);
  }

  @Test
  public void shouldNotCompleteTaskBeforeAllInputsAreDone_AsInputListBefore() throws Exception {
    Task<String> task = Task.named("sync").ofType(String.class)
        .inputs(() -> Arrays.asList(failing, blocking))
        .process(a -> "should not happen");

    awaitBlocked(task);
  }

  @Test
  public void shouldNotCompleteTaskBeforeAllInputsAreDone_AsInputListAfter() throws Exception {
    Task<String> task = Task.named("sync").ofType(String.class)
        .inputs(() -> Arrays.asList(blocking, failing))
        .process(a -> "should not happen");

    awaitBlocked(task);
  }

  private void awaitBlocked(Task<String> task) throws InterruptedException {
    AwaitValue<Throwable> eval = new AwaitValue<>();
    context.evaluate(task).onFail(eval);

    // should not complete before we unblock the blocked promise
    assertThat(eval.await(1, TimeUnit.SECONDS), is(false));

    blocked.awaitAndGet().set(42);
    Throwable throwable = eval.awaitAndGet();

    assertThat(throwable, instanceOf(RuntimeException.class));
    assertThat(throwable.getMessage(), is("failed"));
  }
}
