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

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import com.spotify.flo.AwaitValue;
import com.spotify.flo.EvalContext;
import com.spotify.flo.Task;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Test;

public class AsyncContextFailurePropagationTest {

  @Test
  public void shouldFailTaskIfUpstreamsFail() throws Exception {
    Task<String> failingUpstream = Task.named("Failing").ofType(String.class)
        .process(() -> {
          throw new RuntimeException("failed");
        });

    AtomicInteger count = new AtomicInteger(0);
    Task<String> successfulUpstream = Task.named("Succeeding").ofType(String.class)
        .process(() -> {
          count.incrementAndGet();
          return "foo";
        });

    AtomicBoolean ran = new AtomicBoolean(false);
    Task<String> task = Task.named("Dependent").ofType(String.class)
        .inputs(() -> Arrays.asList(successfulUpstream, failingUpstream))
        .input(() -> successfulUpstream)
        .input(() -> failingUpstream)
        .process((a, b, c) -> {
          ran.set(true);
          return c + b + a;
        });

    AwaitValue<Throwable> val = new AwaitValue<>();
    MemoizingContext.composeWith(EvalContext.sync())
        .evaluate(task)
        .onFail(val);

    assertThat(val.awaitAndGet(), is(instanceOf(RuntimeException.class)));
    assertThat(val.awaitAndGet().getMessage(), is("failed"));
    assertThat(ran.get(), is(false));
    assertThat(count.get(), is(1));
  }
}
