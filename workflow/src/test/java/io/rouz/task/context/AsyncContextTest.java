package io.rouz.task.context;

import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import io.rouz.task.TaskContext;
import io.rouz.task.TaskContext.Promise;
import io.rouz.task.TaskContext.Value;

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

    assertTrue(val.isAvailable());
    assertThat(val.awaitAndGet(), is("hello"));
  }

  @Test
  public void suppliedValueIsEvaluatedOnExecutorThread() throws Exception {
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

  @Test(expected = IllegalStateException.class)
  public void promiseShouldOnlyAllowSetOnce() throws Exception {
    Promise<String> promise = context.promise();
    promise.set("hello");
    promise.set("nope");
  }

  private static final class AwaitingConsumer<T> implements Consumer<T> {

    private final CountDownLatch latch = new CountDownLatch(1);
    private T value;

    @Override
    public void accept(T t) {
      value = t;
      latch.countDown();
    }

    boolean isAvailable() {
      return latch.getCount() == 0;
    }

    T awaitAndGet() throws InterruptedException {
      assertTrue(latch.await(1, TimeUnit.SECONDS));
      return value;
    }
  }
}
