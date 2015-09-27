package io.rouz.task;

import io.rouz.task.dsl.TaskBuilder;

import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class TaskTest {

  @Test
  public void shouldRunAsExpected() throws Exception {
    Task<EvenResult> wasEven = isEven(6);
    Task<EvenResult> madeEven = isEven(5);

    assertThat(wasEven.output(), instanceOf(WasEven.class));
    assertThat(wasEven.output().result(), is(6));
    assertThat(madeEven.output(), instanceOf(MadeEven.class));
    assertThat(madeEven.output().result(), is(10));
  }

  @Test
  public void shouldMemoizeTaskProcessing() throws Exception {
    AtomicInteger counter = new AtomicInteger(0);
    Task<Integer> count = Task.named("Count")
        .process(counter::incrementAndGet);

    Task<Integer> sum = Task.named("Sum")
        .in(() -> count)
        .in(() -> count)
        .in(() -> count)
        .process((a, b, c) -> a + b + c);

    assertThat(sum.output(), is(3));
    assertThat(counter.get(), is(1)); // only called once

    // only memoized during each execution
    assertThat(count.output(), is(2));
    assertThat(count.output(), is(3));
    assertThat(counter.get(), is(3)); // called twice more
  }

  private Task<EvenResult> isEven(int n) {
    TaskBuilder isEven = Task.named("IsEven", n);

    if (n % 2 == 0) {
      return isEven.process(() -> new WasEven(n));
    }

    return isEven
        .in(() -> evenify(n))
        .process(MadeEven::new);
  }

  private Task<Integer> evenify(int n) {
    return Task.named("Evenify", n).process(() -> n * 2);
  }

  // Result ADT
  static abstract class EvenResult {

    private final int result;

    protected EvenResult(int result) {
      this.result = result;
    }

    int result() {
      return result;
    }
  }

  static class WasEven extends EvenResult {

    protected WasEven(int result) {
      super(result);
    }
  }

  static class MadeEven extends EvenResult {

    protected MadeEven(int result) {
      super(result);
    }
  }
}
