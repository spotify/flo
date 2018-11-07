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

import com.spotify.flo.EvalContext;
import com.spotify.flo.Task;
import com.spotify.flo.TaskOutput;
import com.spotify.flo.TaskId;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OverridingContextTest {

  private static final Logger LOG = LoggerFactory.getLogger(OverridingContext.class);

  private static final EvalContext context = OverridingContext.composeWith(sync(), Logging.create(LOG));

  private static final Map<TaskId, Integer> lookup = new HashMap<>();

  private static final TaskOutput<String, Integer> output = new TaskOutput<String, Integer>() {

    @Override
    public String provide(EvalContext evalContext) {
      return null;
    }

    @Override
    public Optional<Integer> lookup(Task<Integer> task) {
      return Optional.ofNullable(lookup.get(task.id()));
    }
  };

  private static int countUpstreamRuns = 0;
  private static int countRootRuns = 0;

  @Before
  public void setUp() {
    countUpstreamRuns = 0;
    countRootRuns = 0;
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
  public void shouldSkipOnExistingLookup() throws InterruptedException, ExecutionException {
    final Task<Integer> task = rootTaskWithUpstreams(
        upstreamCharCount("1"),
        upstreamCharCount("22")
    );

    lookup.put(task.id(), 3);

    CompletableFuture<Integer> future = new CompletableFuture<>();
    context.evaluate(task).consume(future::complete);
    final Integer value = future.get();

    assertThat(value, is(3));
    assertThat(countUpstreamRuns, is(0));
    assertThat(countRootRuns, is(0));
  }

  @Test
  public void shouldSkipUpstreamOnExistingLookup() throws InterruptedException, ExecutionException {
    final Task<Integer> upstream = upstreamCharCount("1");
    final Task<Integer> task = rootTaskWithUpstreams(
        upstream,
        upstreamCharCount("22")
    );

    lookup.put(upstream.id(), 1);

    CompletableFuture<Integer> future = new CompletableFuture<>();
    context.evaluate(task).consume(future::complete);
    final Integer value = future.get();

    assertThat(value, is(3));
    assertThat(countUpstreamRuns, is(1));
    assertThat(countRootRuns, is(1));
  }

  static Task<Integer> rootTaskWithUpstreams(Task<Integer>... upstreams) {
    return Task.named("rootTask", "foo").ofType(Integer.class)
        .output(output)
        .inputs(() -> Arrays.asList(upstreams))
        .process((context, inputs) -> {
          countRootRuns++;
          return inputs.stream().mapToInt(i -> i).sum();
        });
  }

  Task<Integer> upstreamCharCount(String s) {
    return Task.named("charCount", s).ofType(Integer.class)
        .output(output)
        .process((context) -> {
          countUpstreamRuns++;
          return s.length();
        });
  }
}
