package io.rouz.task;

import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.core.Is.is;
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
    gotB.await(100, TimeUnit.MILLISECONDS);
    assertThat(bValue.get(), is("B first"));
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
