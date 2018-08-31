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

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.spotify.flo.EvalContext;
import com.spotify.flo.Task;
import com.spotify.flo.TaskOperator;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class EvalContextTest {

  private static final String PROVIDED = "provided";
  private static final String SPEC = "spec";
  private static final String RESULT = "result";

  @Mock private TaskOperator.Listener listener;
  @Mock private TaskOperator<String, String, String> operator;

  private Task<String> task;

  @Before
  public void setup() {
    task = Task.named("test")
        .ofType(String.class)
        .operator(operator)
        .process(provided -> SPEC);
  }

  @Test
  public void evaluateInternalShouldInvokeOperatorWithListener() {
    final EvalContext baseContext = EvalContext.sync();
    final EvalContext targetContext = new ForwardingEvalContext(EvalContext.sync()) {
      @Override
      public TaskOperator.Listener listener() {
        return listener;
      }
    };

    when(operator.provide(targetContext)).thenReturn(PROVIDED);
    when(operator.perform(SPEC, listener)).thenReturn(RESULT);

    baseContext.evaluateInternal(task, targetContext).get();
    verify(operator).perform(SPEC, listener);
  }
}
