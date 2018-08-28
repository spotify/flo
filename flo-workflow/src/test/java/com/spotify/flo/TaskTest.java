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
import static org.mockito.Mockito.mock;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class TaskTest {

  @Mock TaskOperator<String, String, String> operator1;
  @Mock TaskOperator<String, String, String> operator2;
  @Mock TaskContextStrict<String, String> tcs1;
  @Mock TaskContextStrict<String, String> tcs2;

  @Test
  public void shouldHaveListOfInputs() throws Exception {
    Task<String> task = Task.named("Inputs").ofType(String.class)
        .input(() -> leaf("A"))
        .inputs(() -> asList(leaf("B"), leaf("C")))
        .process((a, bc) -> "constant");

    assertThat(task.inputs(), contains(
        taskId(is(leaf("A").id())),
        taskId(is(leaf("B").id())),
        taskId(is(leaf("C").id()))
    ));
  }

  @Test
  public void shouldHaveListOfTaskContexts() throws Exception {
    final TaskContextGeneric<Object> tc1 = mock(TaskContextGeneric.class);
    final TaskContextGeneric<Object> tc2 = mock(TaskContextGeneric.class);
    Task<String> task = Task.named("Inputs").ofType(String.class)
        .context(tc1)
        .context(tc2)
        .process((a, b) -> "constant");

    assertThat(task.contexts(), contains(tc1, tc2));
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldDisallowMultipleTaskOperators() {
    Task.named("task")
        .ofType(String.class)
        .operator(operator1)
        .operator(operator2)
        .process((a, b) -> { throw new AssertionError(); });
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldDisallowMultipleTaskContextStricts() {
    Task.named("task")
        .ofType(String.class)
        .context(tcs1)
        .context(tcs2)
        .process((a, b) -> { throw new AssertionError(); });
  }

  //  Naming convention for tests
  //  XX
  //  ^^
  //  ||
  //  |`-----> I,L = in, ins
  //  `------> arity
  //    eg 2RD_IL = arity 2 curried direct processed task with single input and list input
  // 0. ===========================================================================================

  @Test
  public void shouldEvaluate0N() throws Exception {
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

  // 1. ===========================================================================================

  @Test
  public void shouldEvaluate1N_I() throws Exception {
    Task<String> task = Task.named("InContext").ofType(String.class)
        .input(() -> leaf("A"))
        .process((a) -> "done: " + a);

    validateEvaluation(task, "done: A", leaf("A"));
  }

  @Test
  public void shouldEvaluate1N_L() throws Exception {
    Task<String> task = Task.named("InContext").ofType(String.class)
        .inputs(() -> asList(leaf("A"), leaf("B")))
        .process((ab) -> "done: " + ab);

    validateEvaluation(task, "done: [A, B]", leaf("A"), leaf("B"));
  }

  // 2. ===========================================================================================

  @Test
  public void shouldEvaluate2N_II() throws Exception {
    Task<String> task = Task.named("InContext").ofType(String.class)
        .input(() -> leaf("A"))
        .input(() -> leaf("B"))
        .process((a, b) -> "done: " + a + " - " + b);

    validateEvaluation(task, "done: A - B", leaf("A"), leaf("B"));
  }

  @Test
  public void shouldEvaluate2N_IL() throws Exception {
    Task<String> task = Task.named("InContext").ofType(String.class)
        .input(() -> leaf("A"))
        .inputs(() -> asList(leaf("B"), leaf("C")))
        .process((a, bc) -> "done: " + a + " - " + bc);

    validateEvaluation(task, "done: A - [B, C]", leaf("A"), leaf("B"), leaf("C"));
  }

  // 3. ===========================================================================================

  @Test
  public void shouldEvaluate3N_III() throws Exception {
    Task<String> task = Task.named("InContext").ofType(String.class)
        .input(() -> leaf("A"))
        .input(() -> leaf("B"))
        .input(() -> leaf("C"))
        .process((a, b, c) -> "done: " + a + " - " + b +" - " + c);

    validateEvaluation(task, "done: A - B - C", leaf("A"), leaf("B"), leaf("C"));
  }

  @Test
  public void shouldEvaluate3N_IIL() throws Exception {
    Task<String> task = Task.named("InContext").ofType(String.class)
        .input(() -> leaf("A"))
        .input(() -> leaf("B"))
        .inputs(() -> asList(leaf("C"), leaf("D")))
        .process((a, b, cd) -> "done: " + a + " - " + b +" - " + cd);

    validateEvaluation(task, "done: A - B - [C, D]", leaf("A"), leaf("B"), leaf("C"), leaf("D"));
  }

  // ==============================================================================================

  @Test
  public void shouldHaveClassOfTaskType() throws Exception {
    Task<String> task0 = Task.named("WithType").ofType(String.class)
        .process(() -> "");
    Task<String> task1 = Task.named("WithType").ofType(String.class)
        .input(() -> leaf("A"))
        .process((a) -> a);
    Task<String> task2 = Task.named("WithType").ofType(String.class)
        .input(() -> leaf("A"))
        .input(() -> leaf("B"))
        .process((a, b) -> a + " - " + b);
    Task<String> task3 = Task.named("WithType").ofType(String.class)
        .input(() -> leaf("A"))
        .input(() -> leaf("B"))
        .input(() -> leaf("C"))
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

  private Task<String> leaf(String s) {
    return Task.named("Leaf", s).ofType(String.class).process(() -> s);
  }
}
