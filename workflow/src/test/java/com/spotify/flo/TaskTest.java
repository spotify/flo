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

import static com.spotify.flo.TestUtils.taskId;
import static java.util.Arrays.asList;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import com.spotify.flo.TaskContext.Promise;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Test;

public class TaskTest {

  @Test
  public void shouldHaveListOfInputs() throws Exception {
    Task<String> task = Task.named("Inputs").ofType(String.class)
        .in(() -> leaf("A"))
        .ins(() -> asList(leaf("B"), leaf("C")))
        .process((a, bc) -> "constant");

    assertThat(task.inputs(), contains(
        taskId(is(leaf("A").id())),
        taskId(is(leaf("B").id())),
        taskId(is(leaf("C").id()))
    ));
  }

  @Test
  public void shouldHaveListOfOperators() throws Exception {
    OpProvider<Object> op1 = tc -> new Object();
    OpProvider<Object> op2 = tc -> new Object();
    Task<String> task = Task.named("Inputs").ofType(String.class)
        .op(op1)
        .op(op2)
        .process((a, b) -> "constant");

    assertThat(task.ops(), contains(op1, op2));
  }

  //  Naming convention for tests
  //  XXX
  //  ^^^
  //  |||
  //  ||`----> D,C = direct, context
  //  |`-----> I,L = in, ins
  //  `------> arity
  //    eg 2RD_IL = arity 2 curried direct processed task with single input and list input
  // 0. ===========================================================================================

  @Test
  public void shouldEvaluate0ND() throws Exception {
    Task<String> task = Task.named("InContext").ofType(String.class)
        .process(() -> "constant");

    AwaitValue<String> val = new AwaitValue<>();
    ControlledBlockingContext context = new ControlledBlockingContext();
    context.evaluate(task).consume(val);

    context.waitFor(task);
    context.release(task);
    context.waitUntilNumConcurrent(0);
    assertThat(val.awaitAndGet(), is("constant"));
  }

  @Test
  public void shouldEvaluate0NC() throws Exception {
    AtomicReference<Promise<String>> promiseRef = new AtomicReference<>();
    Task<String> task = Task.named("InContext").ofType(String.class).processWithContext(tc -> {
      Promise<String> promise = tc.promise();
      promiseRef.set(promise);
      return promise.value();
    });

    validatePromiseEvaluation(task, promiseRef, "");
  }

  // 1. ===========================================================================================

  @Test
  public void shouldEvaluate1ND_I() throws Exception {
    Task<String> task = Task.named("InContext").ofType(String.class)
        .in(() -> leaf("A"))
        .process((a) -> "done: " + a);

    validateEvaluation(task, "done: A", leaf("A"));
  }

  @Test
  public void shouldEvaluate1ND_L() throws Exception {
    Task<String> task = Task.named("InContext").ofType(String.class)
        .ins(() -> asList(leaf("A"), leaf("B")))
        .process((ab) -> "done: " + ab);

    validateEvaluation(task, "done: [A, B]", leaf("A"), leaf("B"));
  }

  @Test
  public void shouldEvaluate1NC_I() throws Exception {
    AtomicReference<Promise<String>> promiseRef = new AtomicReference<>();
    Task<String> task = Task.named("InContext").ofType(String.class)
        .in(() -> leaf("A"))
        .processWithContext((tc, a) -> {
          Promise<String> promise = tc.promise();
          promiseRef.set(promise);
          return promise.value().map(v -> v + " - " + a);
        });

    validatePromiseEvaluation(task, promiseRef, " - A", leaf("A"));
  }

  @Test
  public void shouldEvaluate1NC_L() throws Exception {
    AtomicReference<Promise<String>> promiseRef = new AtomicReference<>();
    Task<String> task = Task.named("InContext").ofType(String.class)
        .ins(() -> asList(leaf("A"), leaf("B")))
        .processWithContext((tc, ab) -> {
          Promise<String> promise = tc.promise();
          promiseRef.set(promise);
          return promise.value().map(v -> v + " - " + ab);
        });

    validatePromiseEvaluation(task, promiseRef, " - [A, B]", leaf("A"), leaf("B"));
  }

  // 2. ===========================================================================================

  @Test
  public void shouldEvaluate2ND_II() throws Exception {
    Task<String> task = Task.named("InContext").ofType(String.class)
        .in(() -> leaf("A"))
        .in(() -> leaf("B"))
        .process((a, b) -> "done: " + a + " - " + b);

    validateEvaluation(task, "done: A - B", leaf("A"), leaf("B"));
  }

  @Test
  public void shouldEvaluate2ND_IL() throws Exception {
    Task<String> task = Task.named("InContext").ofType(String.class)
        .in(() -> leaf("A"))
        .ins(() -> asList(leaf("B"), leaf("C")))
        .process((a, bc) -> "done: " + a + " - " + bc);

    validateEvaluation(task, "done: A - [B, C]", leaf("A"), leaf("B"), leaf("C"));
  }

  @Test
  public void shouldEvaluate2NC_II() throws Exception {
    AtomicReference<Promise<String>> promiseRef = new AtomicReference<>();
    Task<String> task = Task.named("InContext").ofType(String.class)
        .in(() -> leaf("A"))
        .in(() -> leaf("B"))
        .processWithContext((tc, a, b) -> {
          Promise<String> promise = tc.promise();
          promiseRef.set(promise);
          return promise.value().map(v -> v + " - " + a + " - " + b);
        });

    validatePromiseEvaluation(task, promiseRef, " - A - B", leaf("A"), leaf("B"));
  }

  @Test
  public void shouldEvaluate2NC_IL() throws Exception {
    AtomicReference<Promise<String>> promiseRef = new AtomicReference<>();
    Task<String> task = Task.named("InContext").ofType(String.class)
        .in(() -> leaf("A"))
        .ins(() -> asList(leaf("B"), leaf("C")))
        .processWithContext((tc, a, bc) -> {
          Promise<String> promise = tc.promise();
          promiseRef.set(promise);
          return promise.value().map(v -> v + " - " + a + " - " + bc);
        });

    validatePromiseEvaluation(task, promiseRef, " - A - [B, C]", leaf("A"), leaf("B"), leaf("C"));
  }

  // 3. ===========================================================================================

  @Test
  public void shouldEvaluate3ND_III() throws Exception {
    Task<String> task = Task.named("InContext").ofType(String.class)
        .in(() -> leaf("A"))
        .in(() -> leaf("B"))
        .in(() -> leaf("C"))
        .process((a, b, c) -> "done: " + a + " - " + b +" - " + c);

    validateEvaluation(task, "done: A - B - C", leaf("A"), leaf("B"), leaf("C"));
  }

  @Test
  public void shouldEvaluate3ND_IIL() throws Exception {
    Task<String> task = Task.named("InContext").ofType(String.class)
        .in(() -> leaf("A"))
        .in(() -> leaf("B"))
        .ins(() -> asList(leaf("C"), leaf("D")))
        .process((a, b, cd) -> "done: " + a + " - " + b +" - " + cd);

    validateEvaluation(task, "done: A - B - [C, D]", leaf("A"), leaf("B"), leaf("C"), leaf("D"));
  }

  @Test
  public void shouldEvaluate3NC_III() throws Exception {
    AtomicReference<Promise<String>> promiseRef = new AtomicReference<>();
    Task<String> task = Task.named("InContext").ofType(String.class)
        .in(() -> leaf("A"))
        .in(() -> leaf("B"))
        .in(() -> leaf("C"))
        .processWithContext((tc, a, b, c) -> {
          Promise<String> promise = tc.promise();
          promiseRef.set(promise);
          return promise.value().map(v -> v + " - " + a + " - " + b +" - " + c);
        });

    validatePromiseEvaluation(task, promiseRef, " - A - B - C", leaf("A"), leaf("B"), leaf("C"));
  }

  @Test
  public void shouldEvaluate3NC_IIL() throws Exception {
    AtomicReference<Promise<String>> promiseRef = new AtomicReference<>();
    Task<String> task = Task.named("InContext").ofType(String.class)
        .in(() -> leaf("A"))
        .in(() -> leaf("B"))
        .ins(() -> asList(leaf("C"), leaf("D")))
        .processWithContext((tc, a, b, cd) -> {
          Promise<String> promise = tc.promise();
          promiseRef.set(promise);
          return promise.value().map(v -> v + " - " + a + " - " + b +" - " + cd);
        });

    validatePromiseEvaluation(task, promiseRef, " - A - B - [C, D]", leaf("A"), leaf("B"),
                              leaf("C"), leaf("D"));
  }

  // ==============================================================================================

  @Test
  public void shouldHaveClassOfTaskType() throws Exception {
    Task<String> task0 = Task.named("WithType").ofType(String.class)
        .process(() -> "");
    Task<String> task1 = Task.named("WithType").ofType(String.class)
        .in(() -> leaf("A"))
        .process((a) -> a);
    Task<String> task2 = Task.named("WithType").ofType(String.class)
        .in(() -> leaf("A"))
        .in(() -> leaf("B"))
        .process((a, b) -> a + " - " + b);
    Task<String> task3 = Task.named("WithType").ofType(String.class)
        .in(() -> leaf("A"))
        .in(() -> leaf("B"))
        .in(() -> leaf("C"))
        .process((a, b, c) -> a + " - " + b +" - " + c);

    assertThat(task0.type(), equalTo(String.class));
    assertThat(task1.type(), equalTo(String.class));
    assertThat(task2.type(), equalTo(String.class));
    assertThat(task3.type(), equalTo(String.class));
  }

  // Validators ===================================================================================

  private void validateEvaluation(
      Task<String> task,
      String expectedOutput,
      Task... inputs)
      throws InterruptedException {

    AwaitValue<String> val = new AwaitValue<>();
    ControlledBlockingContext context = new ControlledBlockingContext();
    context.evaluate(task).consume(val);

    context.waitFor(task);
    context.release(task);
    context.waitUntilNumConcurrent(inputs.length + 1); // task + inputs
    for (Task input : inputs) {
      assertTrue(context.isWaiting(input));
    }
    assertFalse(val.isAvailable());

    for (Task input : inputs) {
      context.release(input);
    }
    context.waitUntilNumConcurrent(0);
    assertThat(val.awaitAndGet(), is(expectedOutput));
  }

  private void validatePromiseEvaluation(
      Task<String> task,
      AtomicReference<Promise<String>> promiseRef,
      String expectedOutput,
      Task... inputs)
      throws InterruptedException {

    AwaitValue<String> val = new AwaitValue<>();
    ControlledBlockingContext context = new ControlledBlockingContext();
    context.evaluate(task).consume(val);

    context.waitFor(task);
    context.release(task);
    context.waitUntilNumConcurrent(inputs.length + 1); // task + inputs
    for (Task input : inputs) {
      assertTrue(context.isWaiting(input));
    }
    assertFalse(val.isAvailable());

    for (Task input : inputs) {
      context.release(input);
    }
    context.waitUntilNumConcurrent(1); // task will not complete, promise still waiting
    assertFalse(val.isAvailable());

    //noinspection StatementWithEmptyBody
    while (promiseRef.get() == null) {
    }

    promiseRef.get().set("done: from here");
    context.waitUntilNumConcurrent(0);
    assertThat(val.awaitAndGet(), is("done: from here" + expectedOutput));
  }

  Task<String> leaf(String s) {
    return Task.named("Leaf", s).ofType(String.class).process(() -> s);
  }
}
