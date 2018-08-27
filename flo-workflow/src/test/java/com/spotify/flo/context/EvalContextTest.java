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
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.spotify.flo.EvalContext;
import com.spotify.flo.Task;
import com.spotify.flo.TaskOperator;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class EvalContextTest {

  @Mock private TaskOperator.Listener listener;
  @Mock private TaskOperator<String, String, String> operator;
  private Task<String> task;

  @Captor
  private ArgumentCaptor<TaskOperator.Listener> opListenerCaptor;

  @Before
  public void setup() {
    task = Task.named("test")
        .ofType(String.class)
        .operator(operator)
        .process((oprtr) -> oprtr + " result");
  }

  @Test
  public void listenerIsCalled() {
    final EvalContext baseContext = EvalContext.sync();
    final EvalContext targetContext = spy(EvalContext.sync());
    when(targetContext.listener()).thenReturn(listener);

    baseContext.evaluateInternal(task, targetContext).get();

    verify(targetContext, times(1)).listener();
    verify(operator, times(1)).perform(anyObject(), opListenerCaptor.capture());
    assertEquals(opListenerCaptor.getValue(), listener);
  }
}
