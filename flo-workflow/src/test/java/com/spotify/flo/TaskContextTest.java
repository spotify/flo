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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class TaskContextTest {

  private static final BasicTaskContext context1 = spy(new BasicTaskContext("foo"));
  private static final BasicTaskContext context2 = spy(new BasicTaskContext("bar"));

  private static final Injected injected = spy(new Injected());
  private static final TestTaskOutput output = spy(new TestTaskOutput(injected, "bar"));

  private static String setFromInjected;

  @Mock private static TaskBuilder.F0<String> fn1;
  @Mock private static TaskBuilder.F0<String> fn2;

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.initMocks(this);
    setFromInjected = null;
    reset(context1, context2, output, injected, fn1, fn2);
  }

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
    Task<String> task = Task.named("inject").ofType(String.class)
        .context(ForwardingTaskContextGeneric.forwardingContext(() -> context1))
        .output(ForwardingTaskOutput.forwardingOutput(() -> output))
        .process((i1, i2) -> {
          assertThat(i1, is("foo"));
          assertThat(i2, is("bar"));
          context1.mark();
          return i1 + i2;
        });

    evalAndGet(task);
    InOrder inOrder = inOrder(context1, output, injected);
    inOrder.verify(context1).preRun(task);
    inOrder.verify(output).preRun(task);
    inOrder.verify(context1).mark();
    inOrder.verify(context1).onSuccess(task, "foobar");
    inOrder.verify(output).onSuccess(task, "foobar");
    inOrder.verify(injected).doSomething("foobar");
  }

  @Test
  public void lifecycleMethodsCalledInOrderOnFail() throws Exception {
    Task<String> task = Task.named("inject").ofType(String.class)
        .context(ForwardingTaskContextGeneric.forwardingContext(() -> context1))
        .context(ForwardingTaskContextGeneric.forwardingContext(() -> context2))
        .process((i1, i2) -> {
          assertThat(i1, is("foo"));
          assertThat(i2, is("bar"));
          context1.mark();
          throw new RuntimeException("force fail");
        });

    Throwable throwable = evalAndGetException(task);
    InOrder inOrder = inOrder(context1, context2);
    inOrder.verify(context1).preRun(task);
    inOrder.verify(context2).preRun(task);
    inOrder.verify(context1).mark();
    inOrder.verify(context1).onFail(task, throwable);
    inOrder.verify(context2).onFail(task, throwable);
    assertThat(throwable.getMessage(), is("force fail"));
  }

  @Test
  public void lifecycleMethodsCalledAfterInputsHaveEvaluated() throws Exception {
    when(fn1.get()).thenReturn("hej");
    when(fn2.get()).thenReturn("hej");

    Task<String> task = Task.named("inject").ofType(String.class)
        .input(() -> Task.named("foo").ofType(String.class).process(() -> fn1.get()))
        .context(ForwardingTaskContextGeneric.forwardingContext(() -> context1))
        .input(() -> Task.named("bar").ofType(String.class).process(() -> fn2.get()))
        .process((t1, i1, t2) -> {
          context1.mark();
          return t1 + i1 + t2;
        });

    evalAndGet(task);

    InOrder inOrder = inOrder(fn1, fn2, context1);
    inOrder.verify(fn1).get();
    inOrder.verify(context1).provide(any());
    inOrder.verify(fn2).get();
    inOrder.verify(context1).preRun(task);
    inOrder.verify(context1).mark();
    inOrder.verify(context1).onSuccess(task, "hejfoohej");
  }

  @Test
  public void lifecycleMethodsNotCalledIfInputsFail() throws Exception {
    final Task<String> failingInput = Task.named("foo")
        .ofType(String.class)
        .process(() -> {
          throw new RuntimeException("Fail");
        });

    final Task<String> task = Task.named("inject").ofType(String.class)
        .context(ForwardingTaskContextGeneric.forwardingContext(() -> context1))
        .input(() -> failingInput)
        .process((i1, t1) -> {
          context1.mark();
          return t1 + i1;
        });

    final Throwable throwable = evalAndGetException(task);
    assertThat(throwable.getMessage(), is("Fail"));

    verify(context1).provide(any());
    verifyNoMoreInteractions(context1);
  }

  private static class Injected {
    String doSomething(String some) {
      setFromInjected = "something " + some;
      return "ok";
    }
  }

  private static class TestTaskContext extends TaskContextGeneric<Injected> {

    @Override
    public Injected provide(EvalContext evalContext) {
      return new Injected();
    }
  }

  private static class BasicTaskContext extends TaskContextGeneric<String> {

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

  private static class TestTaskOutput extends TaskOutput<String, String> {

    private final Injected injected;
    private final String value;

    private TestTaskOutput(Injected injected, String value) {
      this.injected = injected;
      this.value = value;
    }

    @Override
    public String provide(EvalContext evalContext) {
      return value;
    }

    @Override
    public void onSuccess(Task<?> task, String result) {
      injected.doSomething(result);
    }
  }
}
