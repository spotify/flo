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
import static com.spotify.flo.context.InstrumentedContext.Listener.Phase.FAILURE;
import static com.spotify.flo.context.InstrumentedContext.Listener.Phase.START;
import static com.spotify.flo.context.InstrumentedContext.Listener.Phase.SUCCESS;
import static org.hamcrest.Matchers.contains;
import static org.junit.Assert.assertThat;

import com.google.common.collect.Lists;
import com.spotify.flo.Task;
import com.spotify.flo.EvalContext;
import com.spotify.flo.TaskId;
import java.util.List;
import org.junit.Test;

public class InstrumentedContextTest {

  InstrumentedContext.Listener listener = new InstrumentedContext.Listener() {
    @Override
    public void task(Task<?> task) {
      calls.add("task:" + task.id());
    }

    @Override
    public void status(TaskId task, Phase phase) {
      calls.add("status:" + task + ":" + phase);
    }
  };

  List<String> calls = Lists.newArrayList();
  EvalContext context = InstrumentedContext.composeWith(sync(), listener);

  @Test
  public void callsListenerWithTasksAndStatuses() throws Exception {
    Task<Integer> task = example(7);
    context.evaluate(task);

    TaskId example7 = task.id();
    TaskId upstream7 = upstream(7).id();
    TaskId upstream8 = upstream(8).id();

    assertThat(calls, contains(
        "task:" + example7,
        "task:" + upstream8,
        "status:" + upstream8 + ":" + START,
        "status:" + upstream8 + ":" + SUCCESS,
        "task:" + upstream7,
        "status:" + upstream7 + ":" + START,
        "status:" + upstream7 + ":" + SUCCESS,
        "status:" + example7 + ":" + START,
        "status:" + example7 + ":" + SUCCESS
    ));
  }

  @Test
  public void callsStatusForFailingTask() throws Exception {
    Task<String> failing = failing();
    context.evaluate(failing);

    assertThat(calls, contains(
        "task:" + failing.id(),
        "status:" + failing.id() + ":" + START,
        "status:" + failing.id() + ":" + FAILURE
    ));
  }

  Task<String> upstream(int i) {
    return Task.named("upstream", i).ofType(String.class)
        .process(() -> "upstream" + i);
  }

  Task<Integer> example(int i) {
    return Task.named("example", i).ofType(Integer.class)
        .in(() -> upstream(i))
        .in(() -> upstream(i+1))
        .process((u1, u2) -> u1.length() + u2.length());
  }

  Task<String> failing() {
    return Task.named("failing").ofType(String.class)
        .process(() -> {
          throw new RuntimeException("fail");
        });
  }
}
