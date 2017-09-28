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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import com.spotify.flo.AwaitValue;
import com.spotify.flo.EvalContext;
import com.spotify.flo.EvalContext.Promise;
import com.spotify.flo.EvalContext.Value;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Test;

public class InMemImmediateContextTest {

  EvalContext context = InMemImmediateContext.create();

  @Test
  public void valueContextIsCreatingContext() throws Exception {
    Value<String> value = context.immediateValue("hello");

    assertThat(value.context(), is(context));
  }

  @Test
  public void immediateValueIsAvailable() throws Exception {
    AtomicReference<String> val = new AtomicReference<>();
    context.immediateValue("hello").consume(val::set);

    assertThat(val.get(), is("hello"));
  }

  @Test
  public void promiseShouldCompleteValueWhenSet() throws Exception {
    AtomicReference<String> val = new AtomicReference<>();
    Promise<String> promise = context.promise();
    promise.value().consume(val::set);
    assertNull(val.get());

    promise.set("hello");
    assertThat(val.get(), is("hello"));
  }

  @Test
  public void mappedValueOfPromiseShouldCompleteWhenSet() throws Exception {
    AtomicReference<String> val1 = new AtomicReference<>();
    AtomicReference<String> val2 = new AtomicReference<>();
    Promise<String> promise = context.promise();
    Value<String> value1 = promise.value();
    Value<String> value2 = value1.map(hello -> hello + " world");

    value1.consume(val1::set);
    value2.consume(val2::set);
    assertNull(val1.get());
    assertNull(val2.get());

    promise.set("hello");
    assertThat(val1.get(), is("hello"));
    assertThat(val2.get(), is("hello world"));
  }

  @Test
  public void flatMappedValueOfPromiseShouldLatchAndComplete() throws Exception {
    AtomicReference<String> val1 = new AtomicReference<>();
    AtomicReference<String> val2 = new AtomicReference<>();
    Promise<String> promise1 = context.promise();
    Promise<String> promise2 = context.promise();
    Value<String> value1 = promise1.value();
    Value<String> value2 = value1.flatMap(
        hello -> promise2.value().map(world -> hello + " " + world + "!"));

    value1.consume(val1::set);
    value2.consume(val2::set);
    assertNull(val1.get());
    assertNull(val2.get());

    promise1.set("hello");
    assertThat(val1.get(), is("hello"));
    assertNull(val2.get());

    promise2.set("world");
    assertThat(val2.get(), is("hello world!"));
  }

  @Test
  public void propagateFailure() throws Exception {
    AtomicReference<Throwable> val = new AtomicReference<>();
    Promise<String> promise = context.promise();

    promise.value().onFail(val::set);

    assertThrown(val, promise);
  }

  @Test
  public void propagateFailureThroughMap() throws Exception {
    AtomicReference<Throwable> val = new AtomicReference<>();
    Promise<String> promise = context.promise();

    promise.value()
        .map(s -> s + " world")
        .onFail(val::set);

    assertThrown(val, promise);
  }

  @Test
  public void propagateFailureThroughFlatMap() throws Exception {
    AtomicReference<Throwable> val = new AtomicReference<>();
    Promise<String> promise = context.promise();

    promise.value()
        .flatMap(s -> context.immediateValue(s + " world"))
        .onFail(val::set);

    assertThrown(val, promise);
  }

  @Test
  public void propagateFailureFromInnerValueOnFlatMap() throws Exception {
    AtomicReference<Throwable> val = new AtomicReference<>();
    Promise<String> promise1 = context.promise();
    Promise<String> promise2 = context.promise();

    promise1.value()
        .flatMap(s -> promise2.value().map(s2 -> s + s2))
        .onFail(val::set);

    promise1.set("hello");
    assertThrown(val, promise2);
  }

  @Test
  public void propagateFailureThroughMapAndFlatMapMix() throws Exception {
    AtomicReference<Throwable> val = new AtomicReference<>();
    Promise<String> promise = context.promise();

    promise.value()
        .map(s -> s + " world")
        .flatMap(s -> context.immediateValue(s + " world"))
        .flatMap(s -> context.immediateValue(s + " world"))
        .map(s -> s + " world")
        .map(s -> s + " world")
        .flatMap(s -> context.immediateValue(s + " world"))
        .onFail(val::set);

    assertThrown(val, promise);
  }

  private void assertThrown(AtomicReference<Throwable> val, Promise<String> promise) throws Exception {
    Throwable thrown = new Throwable();
    promise.fail(thrown);
    Throwable throwable = val.get();

    assertTrue(throwable == thrown);
  }

  @Test
  public void consumedValueIsAcceptedOnSameThread() throws Exception {
    String outerThread = Thread.currentThread().getName();
    AwaitValue<String> val = new AwaitValue<>();
    context.immediateValue("hello").consume(val);

    assertThat(val.acceptingThreadName(), is(outerThread));
  }

  @Test
  public void suppliedValueIsComputedOnSameThread() throws Exception {
    String outerThread = Thread.currentThread().getName();
    AtomicReference<String> evalThread = new AtomicReference<>();
    AwaitValue<String> val = new AwaitValue<>();
    context.value(() -> {
      evalThread.set(Thread.currentThread().getName());
      return "hello";
    }).consume(val);

    assertThat(val.awaitAndGet(), is("hello"));
    assertThat(evalThread.get(), is(outerThread));
  }

  @Test
  public void settingPromiseShouldConsumeValueOnSameThread() throws Exception {
    String outerThread = Thread.currentThread().getName();
    AwaitValue<String> val = new AwaitValue<>();
    Promise<String> promise = context.promise();
    promise.value().consume(val);

    promise.set("hello");
    assertThat(val.awaitAndGet(), is("hello"));
    assertThat(val.acceptingThreadName(), is(outerThread));
  }

  @Test
  public void settingMappedValueShouldComputeOnSameThread() throws Exception {
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
    assertThat(val.acceptingThreadName(), is(outerThread));
    assertThat(evalThread1.get(), is(outerThread));
    assertThat(evalThread2.get(), is(outerThread));
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
}
