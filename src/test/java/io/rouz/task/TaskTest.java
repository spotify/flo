package io.rouz.task;

import io.rouz.task.dsl.TaskBuilder;

import org.junit.Test;

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
