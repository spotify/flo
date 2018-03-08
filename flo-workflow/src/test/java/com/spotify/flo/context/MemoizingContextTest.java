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

package com.spotify.flo.context;

import static com.spotify.flo.EvalContext.sync;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import com.google.auto.value.AutoValue;
import com.spotify.flo.AwaitValue;
import com.spotify.flo.EvalContext;
import com.spotify.flo.Task;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Before;
import org.junit.Test;

public class MemoizingContextTest {

  static ExampleValue value;
  static MemoizingContext.Memoizer<ExampleValue> memoizer;

  EvalContext context = MemoizingContext.composeWith(sync());

  int countUpstreamRuns = 0;
  int countExampleRuns = 0;

  @Before
  public void setUp() throws Exception {
    value = null;
    memoizer = new ExampleValueMemoizer();
  }

  @Test
  public void evaluatesAndStores() throws Exception {
    Task<ExampleValue> example = example(7);

    AwaitValue<ExampleValue> await = new AwaitValue<>();
    context.evaluate(example).consume(await);

    ExampleValue evaluatedValue = await.awaitAndGet();
    assertThat(evaluatedValue, is(val("ups7", 7)));
    assertThat(countUpstreamRuns, is(1));
    assertThat(countExampleRuns, is(1));
    assertThat(value, is(val("ups7", 7)));
  }

  @Test(expected = IllegalStateException.class)
  public void evaluatesButFailsToStore() throws InterruptedException {
    context = MemoizingContext.builder(sync())
        .memoizer(new MemoizingContext.Memoizer<ExampleValue>() {
          @Override
          public Optional<ExampleValue> lookup(
              Task<ExampleValue> task) {
            return Optional.empty();
          }

          @Override
          public void store(Task<ExampleValue> task,
                            ExampleValue value) {
            throw new IllegalStateException();
          }
        })
        .build();

    context.evaluate(example(7));
  }

  @Test
  public void evaluatesAndStoresWithExplicitMemoizer() throws Exception {
    context = MemoizingContext.builder(sync())
        .memoizer(memoizer)
        .build();

    Task<ExampleValue> example = example(7);

    AwaitValue<ExampleValue> await = new AwaitValue<>();
    context.evaluate(example).consume(await);

    ExampleValue evaluatedValue = await.awaitAndGet();
    assertThat(evaluatedValue, is(val("ups7", 7)));
    assertThat(countUpstreamRuns, is(1));
    assertThat(countExampleRuns, is(1));
    assertThat(value, is(val("ups7", 7)));
  }

  @Test
  public void shortsEvaluationIfMemoizedValueExists() throws Exception {
    value = val("ups8", 8);

    Task<ExampleValue> example = example(8);

    AwaitValue<ExampleValue> await = new AwaitValue<>();
    context.evaluate(example).consume(await);

    ExampleValue evaluatedValue = await.awaitAndGet();
    assertThat(evaluatedValue, is(val("ups8", 8)));
    assertThat(countUpstreamRuns, is(0));
    assertThat(countExampleRuns, is(0));
  }

  @Test
  public void deDuplicatesSameTasks() throws Exception {
    AtomicInteger counter = new AtomicInteger(0);
    Task<Integer> count = Task.named("Count").ofType(Integer.class)
        .process(counter::incrementAndGet);

    Task<Integer> sum = Task.named("Sum").ofType(Integer.class)
        .input(() -> count)
        .input(() -> count)
        .input(() -> count)
        .process((a, b, c) -> a + b + c);

    AwaitValue<Integer> await = new AwaitValue<>();
    context.evaluate(sum).consume(await);

    assertThat(evalAndGet(sum), is(3));
    assertThat(counter.get(), is(1)); // only called once

    // only memoized during each execution
    context = MemoizingContext.composeWith(sync());
    assertThat(evalAndGet(count), is(2));
    assertThat(evalAndGet(count), is(2));
    assertThat(counter.get(), is(2)); // called once more
  }

  private <T> T evalAndGet(Task<T> task) throws InterruptedException {
    AwaitValue<T> val = new AwaitValue<>();
    context.evaluate(task).consume(val);
    return val.awaitAndGet();
  }

  Task<String> upstream(int i) {
    return Task.named("upstream", i).ofType(String.class)
        .process(() -> {
          countUpstreamRuns++;
          return "ups" + i;
        });
  }

  Task<ExampleValue> example(int i) {
    return Task.named("example", i).ofType(ExampleValue.class)
        .input(() -> upstream(i))
        .input(() -> upstream(i))
        .process((ups, ups1) -> {
          countExampleRuns++;
          return val(ups, i);
        });
  }

  @AutoValue
  public abstract static class ExampleValue {
    abstract String foo();
    abstract int bar();

    @MemoizingContext.Memoizer.Impl
    public static MemoizingContext.Memoizer<ExampleValue> memoizer() {
      return memoizer;
    }
  }

  static ExampleValue val(String foo, int bar) {
    return new AutoValue_MemoizingContextTest_ExampleValue(foo, bar);
  }

  static class ExampleValueMemoizer implements MemoizingContext.Memoizer<ExampleValue> {

    @Override
    public Optional<ExampleValue> lookup(Task<ExampleValue> task) {
      System.out.println("lookup task " + task.id());
      return Optional.ofNullable(MemoizingContextTest.value);
    }

    @Override
    public void store(Task<ExampleValue> task, ExampleValue value) {
      System.out.println("store task " + task.id() + " value = " + value);
      MemoizingContextTest.value = value;
    }
  }
}
