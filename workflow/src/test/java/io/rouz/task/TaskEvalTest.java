package io.rouz.task;

import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import io.rouz.task.TaskContext.Promise;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * Tests that verify the interaction between {@link Task} instances and the {@link TaskContext}.
 */
public class TaskEvalTest {

  Task<String> leaf(String s) {
    return Task.named("Leaf", s).constant(() -> s);
  }

  @Test
  public void shouldInvokeCurriedTaskWhenInputsBecomeAvailable() throws Exception {
    AtomicReference<String> bValue = new AtomicReference<>();
    CountDownLatch gotB = new CountDownLatch(1);
    Task<String> task = Task.named("WithInputs").<String>curryTo()
        .in(() -> leaf("A"))
        .in(() -> leaf("B first"))
        .process(b -> {
          bValue.set(b);
          gotB.countDown();
          return a -> "done: " + a + b;
        });

    ControlledBlockingContext context = new ControlledBlockingContext();
    context.evaluate(task);

    // first wait for the main task to be in progress then release it
    // to trigger upstreams to start evaluating
    context.waitFor(task);
    context.release(task);

    context.waitUntilNumConcurrent(3); // {WithInputs, A, B}
    assertNull(bValue.get());

    context.release(leaf("B first"));
    context.waitUntilNumConcurrent(2); // {WithInputs, A}
    assertTrue(gotB.await(100, TimeUnit.MILLISECONDS));
    assertThat(bValue.get(), is("B first"));
  }

  @Test
  public void shouldEvaluateWithContext0() throws Exception {
    AtomicReference<Promise<String>> promiseRef = new AtomicReference<>();
    Task<String> task = Task.named("InContext").processWithContext(tc -> {
      Promise<String> promise = tc.promise();
      promiseRef.set(promise);
      return promise.value();
    });

    AwaitingConsumer<String> val = new AwaitingConsumer<>();
    ControlledBlockingContext context = new ControlledBlockingContext();
    context.evaluate(task).consume(val);

    context.waitFor(task);
    context.release(task);
    context.waitUntilNumConcurrent(1);
    assertFalse(val.isAvailable());

    //noinspection StatementWithEmptyBody
    while (promiseRef.get() == null) {
    }

    promiseRef.get().set("hello");
    context.waitUntilNumConcurrent(0);
    assertThat(val.awaitAndGet(), is("hello"));
  }

  @Test
  public void shouldEvaluateWithContext1() throws Exception {
    AtomicReference<Promise<String>> promiseRef = new AtomicReference<>();
    Task<String> task = Task.named("InContext")
        .in(() -> leaf("A"))
        .processWithContext((tc, a) -> {
          Promise<String> promise = tc.promise();
          promiseRef.set(promise);
          return promise.value().map(v -> v + " - " + a);
        });

    AwaitingConsumer<String> val = new AwaitingConsumer<>();
    ControlledBlockingContext context = new ControlledBlockingContext();
    context.evaluate(task).consume(val);

    context.waitFor(task);
    context.release(task);
    context.waitUntilNumConcurrent(2); // {InContext, A}
    assertTrue(context.isWaiting(leaf("A")));
    assertFalse(val.isAvailable());

    context.release(leaf("A"));
    context.waitUntilNumConcurrent(1); // InContext will not complete, promise still waiting
    assertFalse(val.isAvailable());

    //noinspection StatementWithEmptyBody
    while (promiseRef.get() == null) {
    }

    promiseRef.get().set("done: from here");
    context.waitUntilNumConcurrent(0);
    assertThat(val.awaitAndGet(), is("done: from here - A"));
  }

  @Test
  public void shouldEvaluateWithContext2() throws Exception {
    AtomicReference<Promise<String>> promiseRef = new AtomicReference<>();
    Task<String> task = Task.named("InContext")
        .in(() -> leaf("A"))
        .in(() -> leaf("B"))
        .processWithContext((tc, a, b) -> {
          Promise<String> promise = tc.promise();
          promiseRef.set(promise);
          return promise.value().map(v -> v + " - " + a + " - " + b);
        });

    AwaitingConsumer<String> val = new AwaitingConsumer<>();
    ControlledBlockingContext context = new ControlledBlockingContext();
    context.evaluate(task).consume(val);

    context.waitFor(task);
    context.release(task);
    context.waitUntilNumConcurrent(3); // {InContext, A, B}
    assertTrue(context.isWaiting(leaf("A")));
    assertTrue(context.isWaiting(leaf("B")));
    assertFalse(val.isAvailable());

    context.release(leaf("A"));
    context.release(leaf("B"));
    context.waitUntilNumConcurrent(1); // InContext will not complete, promise still waiting
    assertFalse(val.isAvailable());

    //noinspection StatementWithEmptyBody
    while (promiseRef.get() == null) {
    }

    promiseRef.get().set("done: from here");
    context.waitUntilNumConcurrent(0);
    assertThat(val.awaitAndGet(), is("done: from here - A - B"));
  }

  @Test
  public void shouldEvaluateWithContext3() throws Exception {
    AtomicReference<Promise<String>> promiseRef = new AtomicReference<>();
    Task<String> task = Task.named("InContext")
        .in(() -> leaf("A"))
        .in(() -> leaf("B"))
        .in(() -> leaf("C"))
        .processWithContext((tc, a, b, c) -> {
          Promise<String> promise = tc.promise();
          promiseRef.set(promise);
          return promise.value().map(v -> v + " - " + a + " - " + b +" - " + c);
        });

    AwaitingConsumer<String> val = new AwaitingConsumer<>();
    ControlledBlockingContext context = new ControlledBlockingContext();
    context.evaluate(task).consume(val);

    context.waitFor(task);
    context.release(task);
    context.waitUntilNumConcurrent(4); // {InContext, A, B, C}
    assertTrue(context.isWaiting(leaf("A")));
    assertTrue(context.isWaiting(leaf("B")));
    assertTrue(context.isWaiting(leaf("C")));
    assertFalse(val.isAvailable());

    context.release(leaf("A"));
    context.release(leaf("B"));
    context.release(leaf("C"));
    context.waitUntilNumConcurrent(1); // InContext will not complete, promise still waiting
    assertFalse(val.isAvailable());

    //noinspection StatementWithEmptyBody
    while (promiseRef.get() == null) {
    }

    promiseRef.get().set("done: from here");
    context.waitUntilNumConcurrent(0);
    assertThat(val.awaitAndGet(), is("done: from here - A - B - C"));
  }

  @Test
  public void shouldEvaluateCurriedWithContext() throws Exception {
    AtomicReference<Promise<String>> promiseRef = new AtomicReference<>();
    Task<String> task = Task.named("InContext").<String>curryToValue()
        .in(() -> leaf("A"))
        .process(tc -> a -> {
          Promise<String> promise = tc.promise();
          promiseRef.set(promise);
          return promise.value().map(v -> v + " - " + a);
        });

    AwaitingConsumer<String> val = new AwaitingConsumer<>();
    ControlledBlockingContext context = new ControlledBlockingContext();
    context.evaluate(task).consume(val);

    context.waitFor(task);
    context.release(task);
    context.waitUntilNumConcurrent(2); // {InContext, A}
    assertTrue(context.isWaiting(leaf("A")));
    assertFalse(val.isAvailable());

    context.release(leaf("A"));
    context.waitUntilNumConcurrent(1); // InContext will not complete, promise still waiting
    assertFalse(val.isAvailable());

    //noinspection StatementWithEmptyBody
    while (promiseRef.get() == null) {
    }

    promiseRef.get().set("done: from here");
    context.waitUntilNumConcurrent(0);
    assertThat(val.awaitAndGet(), is("done: from here - A"));
  }

  @Test
  public void shouldEvaluateMultiLevelCurriedWithContext() throws Exception {
    AtomicReference<Promise<String>> promiseRef = new AtomicReference<>();
    Task<String> task = Task.named("InContext").<String>curryToValue()
        .in(() -> leaf("A"))
        .in(() -> leaf("B"))
        .process(tc -> b -> a -> {
          Promise<String> promise = tc.promise();
          promiseRef.set(promise);
          return promise.value().map(v -> v + " - " + a + " - " + b);
        });

    AwaitingConsumer<String> val = new AwaitingConsumer<>();
    ControlledBlockingContext context = new ControlledBlockingContext();
    context.evaluate(task).consume(val);

    context.waitFor(task);
    context.release(task);
    context.waitUntilNumConcurrent(3); // {InContext, A, B}
    assertTrue(context.isWaiting(leaf("A")));
    assertTrue(context.isWaiting(leaf("B")));
    assertFalse(val.isAvailable());

    context.release(leaf("A"));
    context.release(leaf("B"));
    context.waitUntilNumConcurrent(1); // InContext will not complete, promise still waiting
    assertFalse(val.isAvailable());

    //noinspection StatementWithEmptyBody
    while (promiseRef.get() == null) {
    }

    promiseRef.get().set("done: from here");
    context.waitUntilNumConcurrent(0);
    assertThat(val.awaitAndGet(), is("done: from here - A - B"));
  }

  @Test
  public void shouldEvaluateInputsInParallelForCurriedTask() throws Exception {
    AtomicBoolean processed = new AtomicBoolean(false);
    Task<String> task = Task.named("WithInputs").<String>curryTo()
        .in(() -> leaf("A"))
        .in(() -> leaf("B"))
        .in(() -> leaf("C"))
        .process(c -> b -> a -> {
          processed.set(true);
          return "done: " + a + b + c;
        });

    validateParallelEvaluation(task, processed);
  }

  @Test
  public void shouldEvaluateInputsInParallelForChainedTask() throws Exception {
    AtomicBoolean processed = new AtomicBoolean(false);
    Task<String> task = Task.named("WithInputs")
        .in(() -> leaf("A"))
        .in(() -> leaf("B"))
        .in(() -> leaf("C"))
        .process((a, b, c) -> {
          processed.set(true);
          return "done: " + a + b + c;
        });

    validateParallelEvaluation(task, processed);
  }

  private void validateParallelEvaluation(Task<String> task, AtomicBoolean processed)
      throws InterruptedException {

    ControlledBlockingContext context = new ControlledBlockingContext();
    context.evaluate(task);

    // first wait for the main task to be in progress then release it
    // to trigger upstreams to start evaluating
    context.waitFor(task);
    context.release(task);

    context.waitUntilNumConcurrent(4); // {WithInputs, A, B, C}
    assertTrue(context.isWaiting(leaf("A")));
    assertTrue(context.isWaiting(leaf("B")));
    assertTrue(context.isWaiting(leaf("C")));
    assertFalse(processed.get());

    context.release(leaf("C"));
    context.waitUntilNumConcurrent(3); // {WithInputs, A, B}
    assertTrue(context.isWaiting(leaf("A")));
    assertTrue(context.isWaiting(leaf("B")));
    assertFalse(context.isWaiting(leaf("C")));

    context.release(leaf("B"));
    context.release(leaf("A"));
    context.waitUntilNumConcurrent(0); // WithInputs will also complete here
    assertTrue(processed.get());
  }
}
