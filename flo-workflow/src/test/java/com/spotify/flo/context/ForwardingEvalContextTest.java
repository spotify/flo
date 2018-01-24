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

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.typeCompatibleWith;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import com.spotify.flo.EvalContext;
import com.spotify.flo.EvalContext.Value;
import com.spotify.flo.EvalContextWithTask;
import com.spotify.flo.Fn;
import com.spotify.flo.Task;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class ForwardingEvalContextTest {

  static final Task<String> TASK = Task.create(() -> "", String.class, "test");

  EvalContext delegate = mock(EvalContext.class);
  EvalContext sut = new TestContext(delegate);

  @Test
  public void evaluate() throws Exception {
    sut.evaluate(TASK);

    ArgumentCaptor<EvalContext> contextArgumentCaptor = ArgumentCaptor.forClass(EvalContext.class);
    verify(delegate).evaluateInternal(eq(TASK), contextArgumentCaptor.capture());

    EvalContext capturedContext = contextArgumentCaptor.getValue();
    assertThat(capturedContext.getClass(), typeCompatibleWith(EvalContextWithTask.class));

    EvalContextWithTask wrapperContext = (EvalContextWithTask) capturedContext;
    assertThat(wrapperContext.delegate, is(sut));
  }

  @Test
  public void evaluateInternal() throws Exception {
    sut.evaluateInternal(TASK, delegate);

    verify(delegate).evaluateInternal(TASK, delegate);
  }

  @Test
  public void invokeProcessFn() throws Exception {
    Fn<Value<Object>> fn = () -> null;
    sut.invokeProcessFn(TASK.id(), fn);

    verify(delegate).invokeProcessFn(TASK.id(), fn);
  }

  @Test
  public void value() throws Exception {
    Fn<String> fn = () -> "";
    sut.value(fn);

    verify(delegate).value(fn);
  }

  @Test
  public void immediateValue() throws Exception {
    String value = "";
    sut.immediateValue(value);

    verify(delegate).immediateValue(value);
  }

  @Test
  public void promise() throws Exception {
    sut.promise();

    verify(delegate).promise();
  }

  private static class TestContext extends ForwardingEvalContext {

    TestContext(EvalContext delegate) {
      super(delegate);
    }
  }
}
