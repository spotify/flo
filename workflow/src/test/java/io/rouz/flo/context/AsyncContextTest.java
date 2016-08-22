package io.rouz.flo.context;

import org.junit.Test;

import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;

import io.rouz.flo.AwaitingConsumer;
import io.rouz.flo.TaskContext;
import io.rouz.flo.TaskContext.Promise;
import io.rouz.flo.TaskContext.Value;

import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class AsyncContextTest {

  TaskContext context = AsyncContext.create(Executors.newSingleThreadExecutor());

  @Test
  public void valueContextIsCreatingContext() throws Exception {
    Value<String> value = context.immediateValue("hello");

    assertThat(value.context(), is(context));
  }

  @Test
  public void immediateValueIsAvailable() throws Exception {
    AwaitingConsumer<String> val = new AwaitingConsumer<>();
    context.immediateValue("hello").consume(val);

    assertThat(val.awaitAndGet(), is("hello"));
  }

  @Test
  public void promiseShouldCompleteValueWhenSet() throws Exception {
    AwaitingConsumer<String> val = new AwaitingConsumer<>();
    Promise<String> promise = context.promise();
    promise.value().consume(val);
    assertFalse(val.isAvailable());

    promise.set("hello");
    assertThat(val.awaitAndGet(), is("hello"));
  }

  @Test
  public void mappedValueOfPromiseShouldCompleteWhenSet() throws Exception {
    AwaitingConsumer<String> val1 = new AwaitingConsumer<>();
    AwaitingConsumer<String> val2 = new AwaitingConsumer<>();
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
    AwaitingConsumer<String> val1 = new AwaitingConsumer<>();
    AwaitingConsumer<String> val2 = new AwaitingConsumer<>();
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
    AwaitingConsumer<Throwable> val = new AwaitingConsumer<>();
    Promise<String> promise = context.promise();

    promise.value().onFail(val);

    assertThrown(val, promise);
  }

  @Test
  public void propagateFailureThroughMap() throws Exception {
    AwaitingConsumer<Throwable> val = new AwaitingConsumer<>();
    Promise<String> promise = context.promise();

    promise.value()
        .map(s -> s + " world")
        .onFail(val);

    assertThrown(val, promise);
  }

  @Test
  public void propagateFailureThroughFlatMap() throws Exception {
    AwaitingConsumer<Throwable> val = new AwaitingConsumer<>();
    Promise<String> promise = context.promise();

    promise.value()
        .flatMap(s -> context.immediateValue(s + " world"))
        .onFail(val);

    assertThrown(val, promise);
  }

  @Test
  public void propagateFailureFromInnerValueOnFlatMap() throws Exception {
    AwaitingConsumer<Throwable> val = new AwaitingConsumer<>();
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
    AwaitingConsumer<Throwable> val = new AwaitingConsumer<>();
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

  private void assertThrown(AwaitingConsumer<Throwable> val, Promise<String> promise) throws Exception {
    Throwable thrown = new Throwable();
    promise.fail(thrown);
    Throwable throwable = val.awaitAndGet();

    assertTrue(throwable == thrown);
  }

  @Test
  public void consumedValueIsAcceptedOnExecutorThread() throws Exception {
    String outerThread = Thread.currentThread().getName();
    AwaitingConsumer<String> val = new AwaitingConsumer<>();
    context.immediateValue("hello").consume(val);

    assertThat(val.acceptingThreadName(), not(is(outerThread)));
  }

  @Test
  public void suppliedValueIsComputedOnExecutorThread() throws Exception {
    String outerThread = Thread.currentThread().getName();
    AtomicReference<String> evalThread = new AtomicReference<>();
    AwaitingConsumer<String> val = new AwaitingConsumer<>();
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
    AwaitingConsumer<String> val = new AwaitingConsumer<>();
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
    AwaitingConsumer<String> val = new AwaitingConsumer<>();
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
}
