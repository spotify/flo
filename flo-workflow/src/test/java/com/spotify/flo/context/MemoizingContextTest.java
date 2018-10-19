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
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Before;
import org.junit.Test;

public class MemoizingContextTest {

  private EvalContext context = MemoizingContext.composeWith(sync());

  private static final AtomicInteger counter = new AtomicInteger(0);

  @Before
  public void setUp() throws Exception {
    counter.set(0);
  }

  @Test
  public void deDuplicatesSameTasks() throws Exception {
    Task<Integer> count = Task.named("Count").ofType(Integer.class)
        .process(() -> counter.incrementAndGet());

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

  @AutoValue
  public abstract static class ExampleValue {
    abstract String foo();
    abstract int bar();
  }

}
