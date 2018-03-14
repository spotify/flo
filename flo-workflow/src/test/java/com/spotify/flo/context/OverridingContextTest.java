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
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import com.spotify.flo.AwaitValue;
import com.spotify.flo.EvalContext;
import com.spotify.flo.Task;
import com.spotify.flo.TaskContextStrict;
import com.spotify.flo.TaskId;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.junit.Before;
import org.junit.Test;

public class OverridingContextTest {

  EvalContext context = OverridingContext.composeWith(sync());

  Map<TaskId, Integer> lookup = new HashMap<>();

  TaskContextStrict<String, Integer> taskContext = new TaskContextStrict<String, Integer>() {

    @Override
    public String provide(EvalContext evalContext) {
      return null;
    }

    @Override
    public Optional<Integer> lookup(Task<Integer> task) {
      return Optional.ofNullable(lookup.get(task.id()));
    }
  };

  int countUpstreamRuns = 0;
  int countRootRuns = 0;

  @Before
  public void setUp() {
    lookup.clear();
  }

  @Test
  public void shouldRunEverythingIfLookupsNotFound() {
    final Task<Integer> task = rootTaskWithUpstreams(
        upstreamCharCount("1"),
        upstreamCharCount("22")
    );

    context.evaluate(task);

    assertThat(countUpstreamRuns, is(2));
    assertThat(countRootRuns, is(1));
  }

  @Test
  public void shouldSkipOnExistingLookup() throws InterruptedException {
    final Task<Integer> task = rootTaskWithUpstreams(
        upstreamCharCount("1"),
        upstreamCharCount("22")
    );

    lookup.put(task.id(), 3);

    final AwaitValue<Integer> val = new AwaitValue<>();
    context.evaluate(task).consume(val);
    final Integer value = val.awaitAndGet();

    assertThat(value, is(3));
    assertThat(countUpstreamRuns, is(0));
    assertThat(countRootRuns, is(0));
  }

  @Test
  public void shouldSkipUpstreamOnExistingLookup() throws InterruptedException {
    final Task<Integer> upstream = upstreamCharCount("1");
    final Task<Integer> task = rootTaskWithUpstreams(
        upstream,
        upstreamCharCount("22")
    );

    lookup.put(upstream.id(), 1);

    final AwaitValue<Integer> val = new AwaitValue<>();
    context.evaluate(task).consume(val);
    final Integer value = val.awaitAndGet();

    assertThat(value, is(3));
    assertThat(countUpstreamRuns, is(1));
    assertThat(countRootRuns, is(1));
  }

  Task<Integer> rootTaskWithUpstreams(Task<Integer>... upstreams) {
    return Task.named("rootTask", "foo").ofType(Integer.class)
        .context(taskContext)
        .inputs(() -> Arrays.asList(upstreams))
        .process((context, inputs) -> {
          countRootRuns++;
          return inputs.stream().mapToInt(i -> i).sum();
        });
  }

  Task<Integer> upstreamCharCount(String s) {
    return Task.named("charCount", s).ofType(Integer.class)
        .context(taskContext)
        .process((context) -> {
          countUpstreamRuns++;
          return s.length();
        });
  }
}
