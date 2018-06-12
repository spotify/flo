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

import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import com.spotify.flo.AwaitValue;
import com.spotify.flo.EvalContext;
import com.spotify.flo.EvalContext.Promise;
import com.spotify.flo.EvalContext.Value;
import com.spotify.flo.Fn;
import com.spotify.flo.Task;
import com.spotify.flo.TaskId;
import io.grpc.Context;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Test;

public class AsyncContextTest {

  EvalContext context = AsyncContext.create(Executors.newSingleThreadExecutor());

  @Test
  public void valueContextIsCreatingContext() throws Exception {
    Value<String> value = context.immediateValue("hello");

    assertThat(value.context(), is(context));
  }

  @Test
  public void immediateValueIsAvailable() throws Exception {
    AwaitValue<String> val = new AwaitValue<>();
    context.immediateValue("hello").consume(val);

    assertThat(val.awaitAndGet(), is("hello"));
  }

  @Test
  public void promiseShouldCompleteValueWhenSet() throws Exception {
    AwaitValue<String> val = new AwaitValue<>();
    Promise<String> promise = context.promise();
    promise.value().consume(val);
    assertFalse(val.isAvailable());

    promise.set("hello");
    assertThat(val.awaitAndGet(), is("hello"));
  }

  @Test
  public void mappedValueOfPromiseShouldCompleteWhenSet() throws Exception {
    AwaitValue<String> val1 = new AwaitValue<>();
    AwaitValue<String> val2 = new AwaitValue<>();
    Promise<String> promise = context.promise();
    Value<String> value1 = promise.value();
    Value<String> value2 = value1.map(hello -> hello + " world");

    value1.consume(val1);
    value2.consume(val2);
    assertFalse(val1.isAvailable());
    assertFalse(val2.isAvailable());

    promise.set("hello");
    assertThat(val1.awaitAndGet(), is("hello"));
    assertThat(val2.awaitAndGet(), is("hello world"));
  }

  @Test
  public void flatMappedValueOfPromiseShouldLatchAndComplete() throws Exception {
    AwaitValue<String> val1 = new AwaitValue<>();
    AwaitValue<String> val2 = new AwaitValue<>();
    Promise<String> promise1 = context.promise();
    Promise<String> promise2 = context.promise();
    Value<String> value1 = promise1.value();
    Value<String> value2 = value1.flatMap(
        hello -> promise2.value().map(world -> hello + " " + world + "!"));

    value1.consume(val1);
    value2.consume(val2);
    assertFalse(val1.isAvailable());
    assertFalse(val2.isAvailable());

    promise1.set("hello");
    assertThat(val1.awaitAndGet(), is("hello"));
    assertFalse(val2.isAvailable());

    promise2.set("world");
    assertThat(val2.awaitAndGet(), is("hello world!"));
  }

  @Test
  public void propagateFailure() throws Exception {
    AwaitValue<Throwable> val = new AwaitValue<>();
    Promise<String> promise = context.promise();

    promise.value().onFail(val);

    assertThrown(val, promise);
  }

  @Test
  public void propagateFailureThroughMap() throws Exception {
    AwaitValue<Throwable> val = new AwaitValue<>();
    Promise<String> promise = context.promise();

    promise.value()
        .map(s -> s + " world")
        .onFail(val);

    assertThrown(val, promise);
  }

  @Test
  public void propagateFailureThroughFlatMap() throws Exception {
    AwaitValue<Throwable> val = new AwaitValue<>();
    Promise<String> promise = context.promise();

    promise.value()
        .flatMap(s -> context.immediateValue(s + " world"))
        .onFail(val);

    assertThrown(val, promise);
  }

  @Test
  public void propagateFailureFromInnerValueOnFlatMap() throws Exception {
    AwaitValue<Throwable> val = new AwaitValue<>();
    Promise<String> promise1 = context.promise();
    Promise<String> promise2 = context.promise();

    promise1.value()
        .flatMap(s -> promise2.value().map(s2 -> s + s2))
        .onFail(val);

    promise1.set("hello");
    assertThrown(val, promise2);
  }

  @Test
  public void propagateFailureThroughMapAndFlatMapMix() throws Exception {
    AwaitValue<Throwable> val = new AwaitValue<>();
    Promise<String> promise = context.promise();

    promise.value()
        .map(s -> s + " world")
        .flatMap(s -> context.immediateValue(s + " world"))
        .flatMap(s -> context.immediateValue(s + " world"))
        .map(s -> s + " world")
        .map(s -> s + " world")
        .flatMap(s -> context.immediateValue(s + " world"))
        .onFail(val);

    assertThrown(val, promise);
  }

  private void assertThrown(AwaitValue<Throwable> val, Promise<String> promise) throws Exception {
    Throwable thrown = new Throwable();
    promise.fail(thrown);
    Throwable throwable = val.awaitAndGet();

    assertTrue(throwable == thrown);
  }

  @Test
  public void consumedValueIsAcceptedOnExecutorThread() throws Exception {
    String outerThread = Thread.currentThread().getName();
    AwaitValue<String> val = new AwaitValue<>();
    context.immediateValue("hello").consume(val);

    assertThat(val.acceptingThreadName(), not(is(outerThread)));
  }

  @Test
  public void suppliedValueIsComputedOnExecutorThread() throws Exception {
    String outerThread = Thread.currentThread().getName();
    AtomicReference<String> evalThread = new AtomicReference<>();
    AwaitValue<String> val = new AwaitValue<>();
    context.value(() -> {
      evalThread.set(Thread.currentThread().getName());
      return "hello";
    }).consume(val);

    assertThat(val.awaitAndGet(), is("hello"));
    assertThat(evalThread.get(), not(is(outerThread)));
  }

  @Test
  public void settingPromiseShouldConsumeValueOnExecutorThread() throws Exception {
    String outerThread = Thread.currentThread().getName();
    AwaitValue<String> val = new AwaitValue<>();
    Promise<String> promise = context.promise();
    promise.value().consume(val);

    promise.set("hello");
    assertThat(val.awaitAndGet(), is("hello"));
    assertThat(val.acceptingThreadName(), not(is(outerThread)));
  }

  @Test
  public void settingMappedValueShouldComputeOnExecutorThread() throws Exception {
    String outerThread = Thread.currentThread().getName();
    AtomicReference<String> evalThread1 = new AtomicReference<>();
    AtomicReference<String> evalThread2 = new AtomicReference<>();
    AwaitValue<String> val = new AwaitValue<>();
    Promise<String> promise = context.promise();

    promise.value()
        .map(hello -> {
          evalThread1.set(Thread.currentThread().getName());
          return hello + " world";
        })
        .flatMap(helloWorld -> {
          evalThread2.set(Thread.currentThread().getName());
          return context.immediateValue(helloWorld + "!");
        })
        .consume(val);

    promise.set("hello");
    assertThat(val.awaitAndGet(), is("hello world!"));
    assertThat(val.acceptingThreadName(), not(is(outerThread)));
    assertThat(evalThread1.get(), not(is(outerThread)));
    assertThat(evalThread2.get(), not(is(outerThread)));
  }

  @Test(expected = IllegalStateException.class)
  public void promiseShouldOnlyAllowSetOnce() throws Exception {
    Promise<String> promise = context.promise();
    promise.set("hello");
    promise.set("nope");
  }

  @Test(expected = IllegalStateException.class)
  public void promiseShouldOnlyAllowFailOnce() throws Exception {
    Promise<String> promise = context.promise();
    promise.fail(new Throwable());
    promise.fail(new Throwable());
  }

  @Test
  public void grpcContextIsPropagated() throws Exception {
    final Context.Key<String> key = Context.key("foo");
    final String value = "bar";
    final Context grpcContext = Context.current().withValue(key, value);

    // #value()
    {
      final CompletableFuture<String> future = new CompletableFuture<>();
      grpcContext.call(() -> context.value(key::get)).consume(future::complete);
      assertThat(future.get(30, TimeUnit.SECONDS), is(value));
    }

    // #evaluate()
    {
      final Task<String> task = Task.named("test").ofType(String.class)
          .process(key::get);
      final CompletableFuture<String> future = new CompletableFuture<>();
      grpcContext.call(() -> context.evaluate(task)).consume(future::complete);
      assertThat(future.get(30, TimeUnit.SECONDS), is(value));
    }

    // #invokeProcessFn()
    {
      final CompletableFuture<String> future = new CompletableFuture<>();
      final Fn<String> processFn = key::get;
      grpcContext.call(() -> context.invokeProcessFn(TaskId.create("test"), processFn)).consume(future::complete);
      assertThat(future.get(30, TimeUnit.SECONDS), is(value));
    }
  }
}
