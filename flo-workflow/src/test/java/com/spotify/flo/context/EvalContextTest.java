/*-
 * -\-\-
 * Flo Workflow Definition
 * --
 * Copyright (C) 2016 - 2018 Spotify AB
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

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.spotify.flo.EvalContext;
import com.spotify.flo.Task;
import com.spotify.flo.TaskOperator;
import com.spotify.flo.context.InstrumentedContext.Listener;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class EvalContextTest {

  private Listener listener = mock(Listener.class);
  private TaskOperator operator = mock(TaskOperator.class);
  private Task task = Task.named("test")
      .ofType(String.class)
      .operator(operator)
      .process((op) -> "result");

  private ArgumentCaptor<TaskOperator.Listener> opListenerCaptor = ArgumentCaptor
      .forClass(TaskOperator.Listener.class);

  @Test
  public void listenerIsCalled() {
    final EvalContext syncContext = EvalContext.sync();
    final EvalContext composedContext = spy(InstrumentedContext.composeWith(syncContext, listener));

    syncContext.evaluateInternal(task, composedContext).get();

    verify(composedContext, times(1)).listener();
    verify(operator, times(1)).perform(anyObject(), opListenerCaptor.capture());
    assertEquals(opListenerCaptor.getValue(), composedContext.listener());
  }
}
