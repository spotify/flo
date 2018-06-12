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

import static com.spotify.flo.TestUtils.evalAndGet;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.junit.Test;

/**
 * Tests that verify the interaction between {@link Task} instances and the {@link EvalContext}.
 */
public class TaskEvalBehaviorTest {

  @Test
  public void shouldRunAsExpected() throws Exception {
    Task<EvenResult> wasEven = isEven(6);
    Task<EvenResult> madeEven = isEven(5);

    assertThat(evalAndGet(wasEven), instanceOf(WasEven.class));
    assertThat(evalAndGet(wasEven).result(), is(6));
    assertThat(evalAndGet(madeEven), instanceOf(MadeEven.class));
    assertThat(evalAndGet(madeEven).result(), is(10));
  }

  @Test
  public void shouldHandleStreamParameters() throws Exception {
    Fn<Task<Integer>> countSupplier = countConstructor();

    // 1,2,3,4,5
    List<Task<Integer>> fiveInts = Stream
        .generate(countSupplier)
        .limit(5)
        .collect(toList());

    Task<Integer> sum = Task.named("Sum").ofType(Integer.class)
        .inputs(() -> fiveInts)
        .process(this::sumInts);

    // 1+2+3+4+5 = 15
    assertThat(evalAndGet(sum), is(15));
  }

  @Test
  public void shouldHandleMixedStreamAndPlainParameters() throws Exception {
    Fn<Task<Integer>> countSupplier = countConstructor();

    // 1,2,3,4,5
    List<Task<Integer>> fiveInts = Stream
        .generate(countSupplier)
        .limit(5)
        .collect(toList());

    Task<Integer> sum = Task.named("Sum").ofType(Integer.class)
        .input(() -> isEven(5))
        .inputs(() -> fiveInts)
        .input(() -> isEven(2))
        .process((a, ints, b) -> a.result() + sumInts(ints) + b.result());

    // (5*2) + (1+2+3+4+5) + 2 = 27
    assertThat(evalAndGet(sum), is(27));
  }

  @Test
  public void shouldHandleMultipleStreamParameters() throws Exception {
    Fn<Task<Integer>> countSupplier = countConstructor();

    Fn<List<Task<Integer>>> fiveInts = () -> Stream
        .generate(countSupplier)
        .limit(5)
        .collect(toList());

    Task<Integer> sum = Task.named("Sum").ofType(Integer.class)
        .inputs(fiveInts)
        .inputs(fiveInts)
        .process((first5, second5) -> sumInts(first5) + sumInts(second5));

    // (1+2+3+4+5) + (6+7+8+9+10) = 55
    assertThat(evalAndGet(sum), is(55));
  }

  @Test
  public void shouldOnlyEvaluateInputsParameterOnce() throws Exception {
    Fn<Task<Integer>> countSupplier = countConstructor();

    Task<Integer> sum = Task.named("Sum").ofType(Integer.class)
        .input(countSupplier)
        .input(countSupplier)
        .input(countSupplier)
        .process((a, b, c) -> a + b + c);

    // dummy run
    evalAndGet(sum);

    // 1+2+3 = 6
    assertThat(evalAndGet(sum), is(6));
  }

  @Test
  public void shouldOnlyEvaluateStreamParameterOnce() throws Exception {
    Fn<Task<Integer>> countSupplier = countConstructor();

    Fn<List<Task<Integer>>> fiveInts = () -> Stream
        .generate(countSupplier)
        .limit(5)
        .collect(toList());

    Task<Integer> sum = Task.named("Sum").ofType(Integer.class)
        .inputs(fiveInts)
        .inputs(fiveInts)
        .inputs(fiveInts)
        .process((first5, second5, third5) -> sumInts(first5) + sumInts(second5) + sumInts(third5));

    // dummy run
    evalAndGet(sum);

    // (1+2+3+4+5) + (6+7+8+9+10) + (11+12+13+14+15) = 120
    assertThat(evalAndGet(sum), is(120));
  }

  @Test
  public void shouldListInputIds() throws Exception {
    Task<String> top = Task.named("Top").ofType(String.class)
        .input(() -> isEven(0))
        .input(() -> isEven(1))
        .input(() -> isEven(3))
        .process((a, b, c) -> "done");

    List<TaskId> inputs = top.inputs().stream().map(Task::id).collect(toList());

    TaskId isEven0Id = isEven(0).id();
    TaskId isEven1Id = isEven(1).id();
    TaskId isEven3Id = isEven(3).id();

    assertThat(inputs, contains(isEven0Id, isEven1Id, isEven3Id));
  }

  @Test
  public void shouldListInputsLazily() throws Exception {
    Fn<Task<Integer>> countSupplier = countConstructor();

    Task<Integer> sum = Task.named("Sum").ofType(Integer.class)
        .input(countSupplier)
        .input(countSupplier)
        .input(countSupplier)
        .process((a, b, c) -> a + b + c);

    // pre fetch one
    Task<Integer> one = countSupplier.get();

    // both run and and get inputs
    evalAndGet(sum);
    List<TaskId> inputs = sum.inputs().stream().map(Task::id).collect(toList());

    assertThat(inputs.get(0).toString(), startsWith("Count(4)"));
    assertThat(inputs.get(1).toString(), startsWith("Count(3)"));
    assertThat(inputs.get(2).toString(), startsWith("Count(2)"));

    assertThat(evalAndGet(one), is(1));
    assertThat(evalAndGet(sum), is(9)); // 2+3+4 = 9
  }

  @Test
  public void shouldFlattenListParametersToSameAsIndividual() throws Exception {
    final Task<EvenResult> isEven0 = isEven(0);
    final Task<EvenResult> isEven1 = isEven(1);

    Task<String> list = Task.named("Top").ofType(String.class)
        .inputs(() -> asList(isEven0, isEven1))
        .process(results -> "done " + results.size());

    Task<String> individual = Task.named("Top").ofType(String.class)
        .input(() -> isEven0)
        .input(() -> isEven1)
        .process((in0, in1) -> "done");

    List<TaskId> listInputs = list.inputs()
        .stream()
        .map(Task::id)
        .collect(toList());

    List<TaskId> individualInputs = individual.inputs()
        .stream()
        .map(Task::id)
        .collect(toList());

    assertThat(listInputs, containsInOrder(isEven0.id(), isEven1.id()));
    assertThat(listInputs, equalTo(individualInputs));
  }

  @Test
  public void shouldFlattenListParameters() throws Exception {
    final Task<EvenResult> isEven0 = isEven(0);
    final Task<EvenResult> isEven1 = isEven(1);
    Task<String> top = Task.named("Top").ofType(String.class)
        .inputs(() -> asList(isEven0, isEven1))
        .process(results -> "done " + results.size());

    List<TaskId> taskIds = top.inputs()
        .stream()
        .map(Task::id)
        .collect(toList());

    assertThat(taskIds, containsInOrder(isEven0.id(), isEven1.id()));
  }

  @Test
  public void shouldEvaluateInputsInParallelForChainedTask() throws Exception {
    AtomicBoolean processed = new AtomicBoolean(false);
    Task<String> task = Task.named("WithInputs").ofType(String.class)
        .input(() -> leaf("A"))
        .input(() -> leaf("B"))
        .input(() -> leaf("C"))
        .process((a, b, c) -> {
          processed.set(true);
          return "done: " + a + b + c;
        });

    validateParallelEvaluation(task, processed);
  }

  private void validateParallelEvaluation(Task<String> task, AtomicBoolean processed)
      throws InterruptedException {

    ControlledBlockingContext context = new ControlledBlockingContext();
    context.evaluate(task);

    // first wait for the main task to be in progress then release it
    // to trigger upstreams to start evaluating
    context.waitFor(task);
    context.release(task);

    context.waitUntilNumConcurrent(4); // {WithInputs, A, B, C}
    assertTrue(context.isWaiting(leaf("A")));
    assertTrue(context.isWaiting(leaf("B")));
    assertTrue(context.isWaiting(leaf("C")));
    assertFalse(processed.get());

    context.release(leaf("C"));
    context.waitUntilNumConcurrent(3); // {WithInputs, A, B}
    assertTrue(context.isWaiting(leaf("A")));
    assertTrue(context.isWaiting(leaf("B")));
    assertFalse(context.isWaiting(leaf("C")));

    context.release(leaf("B"));
    context.release(leaf("A"));
    context.waitUntilNumConcurrent(0); // WithInputs will also complete here
    assertTrue(processed.get());
  }

  @Test
  public void shouldInterceptProcessFunctionInContext0() throws Exception {
    Task<String> top = Task.named("Top").ofType(String.class)
        .process(() -> "done");

    validateInterception(top, "done");
  }

  @Test
  public void shouldInterceptProcessFunctionInContext1() throws Exception {
    Task<String> top = Task.named("Top").ofType(String.class)
        .input(() -> leaf("A"))
        .process((a) -> "done");

    validateInterception(top, "done", leaf("A"));
  }

  @Test
  public void shouldInterceptProcessFunctionInContext1L() throws Exception {
    Task<String> top = Task.named("Top").ofType(String.class)
        .inputs(() -> singletonList(leaf("A")))
        .process((a) -> "done");

    validateInterception(top, "done", leaf("A"));
  }

  @Test
  public void shouldInterceptProcessFunctionInContext2() throws Exception {
    Task<String> top = Task.named("Top").ofType(String.class)
        .input(() -> leaf("A"))
        .input(() -> leaf("B"))
        .process((a, b) -> "done");

    validateInterception(top, "done", leaf("A"), leaf("B"));
  }

  @Test
  public void shouldInterceptProcessFunctionInContext2L() throws Exception {
    Task<String> top = Task.named("Top").ofType(String.class)
        .inputs(() -> singletonList(leaf("A")))
        .inputs(() -> singletonList(leaf("B")))
        .process((a, b) -> "done");

    validateInterception(top, "done", leaf("A"), leaf("B"));
  }

  @Test
  public void shouldInterceptProcessFunctionInContext3() throws Exception {
    Task<String> top = Task.named("Top").ofType(String.class)
        .input(() -> leaf("A"))
        .input(() -> leaf("B"))
        .input(() -> leaf("C"))
        .process((a, b, c) -> "done");

    validateInterception(top, "done", leaf("A"), leaf("B"), leaf("C"));
  }

  private void validateInterception(Task<String> task, String res, Task<?>... inputs)
      throws Exception {
    AtomicBoolean intercepted = new AtomicBoolean(false);

    // gating mechanism used in ControlledBlockingContext to implement intercepts
    ControlledBlockingContext context = new ControlledBlockingContext();
    context.intercept(task, valueFn -> {
      intercepted.set(true);
      return valueFn.get().map(done -> "!!" + done + "!!");
    });

    AwaitValue<String> val = new AwaitValue<>();
    context.evaluate(task).consume(val);

    context.waitFor(task);
    context.release(task);

    // release inputs one at a time and verify interception does not happen
    for (int i = 0; i < inputs.length; i++) {
      context.waitUntilNumConcurrent(1 + inputs.length - i);
      for (int j = i; j < inputs.length; j++) {
        assertThat(context.waitingTasks(), hasItem(inputs[j].id()));
      }
      assertFalse(intercepted.get());

      context.release(inputs[i]);
    }

    assertThat(val.awaitAndGet(), is("!!" + res + "!!"));
    assertTrue(intercepted.get());
  }

  private Task<String> leaf(String s) {
    return Task.named("Leaf", s).ofType(String.class).process(() -> s);
  }

  private Fn<Task<Integer>> countConstructor() {
    AtomicInteger counter = new AtomicInteger(0);
    return () -> {
      int n = counter.incrementAndGet();
      return Task.named("Count", n).ofType(Integer.class)
          .process(() -> n);
    };
  }

  private int sumInts(List<Integer> intsList) {
    return intsList.stream().reduce(0, (a, b) -> a + b);
  }

  private Task<EvenResult> isEven(int n) {
    TaskBuilder<EvenResult> isEven = Task.named("IsEven", n).ofType(EvenResult.class);

    if (n % 2 == 0) {
      return isEven.process(() -> new WasEven(n));
    }

    return isEven
        .input(() -> evenify(n))
        .process(MadeEven::new);
  }

  private Task<Integer> evenify(int n) {
    return Task.named("Evenify", n).ofType(Integer.class)
        .process(() -> n * 2);
  }

  // Result ADT
  abstract class EvenResult {

    private final int result;

    EvenResult(int result) {
      this.result = result;
    }

    int result() {
      return result;
    }
  }

  class WasEven extends EvenResult {

    WasEven(int result) {
      super(result);
    }
  }

  class MadeEven extends EvenResult {

    MadeEven(int result) {
      super(result);
    }
  }

  private static <T> Matcher<Iterable<? extends T>> containsInOrder(T a, T b) {
    Objects.requireNonNull(a);
    Objects.requireNonNull(b);
    return new TypeSafeMatcher<Iterable<? extends T>>() {

      @Override
      protected boolean matchesSafely(Iterable<? extends T> ts) {
        int ai = -1, bi = -1, i = 0;
        for (T t : ts) {
          if (a.equals(t)) {
            ai = i;
          }
          if (b.equals(t)) {
            bi = i;
          }
          i++;
        }

        return ai > -1 && bi > -1 && ai < bi;
      }

      @Override
      public void describeTo(Description description) {
        description.appendText("Contains ");
        description.appendValue(a);
        description.appendText(" before ");
        description.appendValue(b);
      }
    };
  }
}
