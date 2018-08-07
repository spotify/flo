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

package com.spotify.flo;

import static com.spotify.flo.TestUtils.evalAndGet;
import static com.spotify.flo.TestUtils.evalAndGetException;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import com.spotify.flo.TaskBuilder.F0;
import org.junit.Test;
import org.mockito.InOrder;

public class TaskContextTest {

  private String setFromInjected;

  @Test
  public void injectsTaskContexts() throws Exception {
    Task<String> task = Task.named("inject").ofType(String.class)
        .context(new TestTaskContext())
        .process(injected -> injected.doSomething("foo"));

    String result = evalAndGet(task);

    assertThat(result, is("ok"));
    assertThat(setFromInjected, is("something foo"));
  }

  @Test
  public void injectsTaskContextsSecond() throws Exception {
    Task<String> task = Task.named("inject").ofType(String.class)
        .input(() -> Task.named("foo").ofType(String.class).process(() -> "hej"))
        .context(new TestTaskContext())
        .process((a, b) -> b.doSomething("bar"));

    String result = evalAndGet(task);

    assertThat(result, is("ok"));
    assertThat(setFromInjected, is("something bar"));
  }

  @Test
  public void lifecycleMethodsCalledInOrder() throws Exception {
    BasicTaskContext tc1 = spy(new BasicTaskContext("foo"));
    Injected injected = spy(new Injected());
    strictTaskContext tc2 = spy(new strictTaskContext(injected, "bar"));
    Task<String> task = Task.named("inject").ofType(String.class)
        .context(tc1)
        .context(tc2)
        .process((i1, i2) -> {
          assertThat(i1, is("foo"));
          assertThat(i2, is("bar"));
          tc1.mark();
          return i1 + i2;
        });

    evalAndGet(task);
    InOrder inOrder = inOrder(tc1, tc2, injected);
    inOrder.verify(tc1).preRun(task);
    inOrder.verify(tc2).preRun(task);
    inOrder.verify(tc1).mark();
    inOrder.verify(tc1).onSuccess(task, "foobar");
    inOrder.verify(tc2).onSuccess(task, "foobar");
    inOrder.verify(injected).doSomething("foobar");
  }

  @Test
  public void lifecycleMethodsCalledInOrderOnFail() throws Exception {
    BasicTaskContext tc1 = spy(new BasicTaskContext("foo"));
    BasicTaskContext tc2 = spy(new BasicTaskContext("bar"));
    Task<String> task = Task.named("inject").ofType(String.class)
        .context(tc1)
        .context(tc2)
        .process((i1, i2) -> {
          assertThat(i1, is("foo"));
          assertThat(i2, is("bar"));
          tc1.mark();
          throw new RuntimeException("force fail");
        });

    Throwable throwable = evalAndGetException(task);
    InOrder inOrder = inOrder(tc1, tc2);
    inOrder.verify(tc1).preRun(task);
    inOrder.verify(tc2).preRun(task);
    inOrder.verify(tc1).mark();
    inOrder.verify(tc1).onFail(task, throwable);
    inOrder.verify(tc2).onFail(task, throwable);
    assertThat(throwable.getMessage(), is("force fail"));
  }

  @Test
  public void lifecycleMethodsCalledAfterInputsHaveEvaluated() throws Exception {
    //noinspection unchecked
    TaskBuilder.F0<String> t1Fn = mock(TaskBuilder.F0.class);
    //noinspection unchecked
    TaskBuilder.F0<String> t2Fn = mock(TaskBuilder.F0.class);
    when(t1Fn.get()).thenReturn("hej");
    when(t2Fn.get()).thenReturn("hej");
    BasicTaskContext tc1 = spy(new BasicTaskContext("foo"));

    Task<String> task = Task.named("inject").ofType(String.class)
        .input(() -> Task.named("foo").ofType(String.class).process(t1Fn))
        .context(tc1)
        .input(() -> Task.named("bar").ofType(String.class).process(t2Fn))
        .process((t1, i1, t2) -> {
          tc1.mark();
          return t1 + i1 + t2;
        });

    evalAndGet(task);

    InOrder inOrder = inOrder(t1Fn, t2Fn, tc1);
    inOrder.verify(t1Fn).get();
    inOrder.verify(tc1).provide(any());
    inOrder.verify(t2Fn).get();
    inOrder.verify(tc1).preRun(task);
    inOrder.verify(tc1).mark();
    inOrder.verify(tc1).onSuccess(task, "hejfoohej");
  }

  @Test
  public void lifecycleMethodsNotCalledIfInputsFail() throws Exception {
    //noinspection unchecked
    TaskBuilder.F0<String> t1Fn = mock(F0.class);
    when(t1Fn.get()).thenThrow(new RuntimeException("Fail"));
    BasicTaskContext tc1 = spy(new BasicTaskContext("foo"));

    Task<String> task = Task.named("inject").ofType(String.class)
        .context(tc1)
        .input(() -> Task.named("foo").ofType(String.class).process(t1Fn))
        .process((i1, t1) -> {
          tc1.mark();
          return t1 + i1;
        });

    Throwable throwable = evalAndGetException(task);
    assertThat(throwable.getMessage(), is("Fail"));

    InOrder inOrder = inOrder(t1Fn, tc1);
    inOrder.verify(tc1).provide(any());
    inOrder.verify(t1Fn).get();
    inOrder.verifyNoMoreInteractions();
  }

  private class Injected {
    String doSomething(String some) {
      setFromInjected = "something " + some;
      return "ok";
    }
  }

  private class TestTaskContext extends TaskContextGeneric<Injected> {

    @Override
    public Injected provide(EvalContext evalContext) {
      return new Injected();
    }
  }

  private class BasicTaskContext extends TaskContextGeneric<String> {

    private final String inject;

    private BasicTaskContext(String inject) {
      this.inject = inject;
    }

    @Override
    public String provide(EvalContext evalContext) {
      return inject;
    }

    public void mark() {
      // noop, used to verify call order
    }
  }

  private class strictTaskContext extends TaskContextStrict<String, String> {

    private final Injected injected;
    private final String value;

    private strictTaskContext(Injected injected, String value) {
      this.injected = injected;
      this.value = value;
    }

    @Override
    public String provide(EvalContext evalContext) {
      return value;
    }

    @Override
    public void onSuccessStrict(Task<?> task, String result) {
      injected.doSomething(result);
    }
  }
}
