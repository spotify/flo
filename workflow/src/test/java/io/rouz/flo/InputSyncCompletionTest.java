package io.rouz.flo;

import org.junit.Test;

import java.util.Arrays;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class InputSyncCompletionTest {

  TaskContext context = TaskContext.async(Executors.newFixedThreadPool(2));

  AwaitingConsumer<TaskContext.Promise<Integer>> blocked = new AwaitingConsumer<>();
  Task<Integer> blocking = Task.named("blocking").ofType(Integer.class)
      .processWithContext((tc) -> {
        TaskContext.Promise<Integer> promise = tc.promise();
        blocked.accept(promise);
        return promise.value();
      });

  Task<Integer> failing = Task.named("failing").ofType(Integer.class)
      .process(() -> {
        throw new RuntimeException("failed");
      });

  @Test
  public void shouldNotCompleteTaskBeforeAllInputsAreDone_FailingBefore() throws Exception {
    Task<String> task = Task.named("sync").ofType(String.class)
        .in(() -> failing)
        .in(() -> blocking)
        .process((a, b) -> "should not happen");

    awaitBlocked(task);
  }

  @Test
  public void shouldNotCompleteTaskBeforeAllInputsAreDone_FailingAfter() throws Exception {
    Task<String> task = Task.named("sync").ofType(String.class)
        .in(() -> blocking)
        .in(() -> failing)
        .process((a, b) -> "should not happen");

    awaitBlocked(task);
  }

  @Test
  public void shouldNotCompleteTaskBeforeAllInputsAreDone_AsInputListBefore() throws Exception {
    Task<String> task = Task.named("sync").ofType(String.class)
        .ins(() -> Arrays.asList(failing, blocking))
        .process(a -> "should not happen");

    awaitBlocked(task);
  }

  @Test
  public void shouldNotCompleteTaskBeforeAllInputsAreDone_AsInputListAfter() throws Exception {
    Task<String> task = Task.named("sync").ofType(String.class)
        .ins(() -> Arrays.asList(blocking, failing))
        .process(a -> "should not happen");

    awaitBlocked(task);
  }

  private void awaitBlocked(Task<String> task) throws InterruptedException {
    AwaitingConsumer<Throwable> eval = new AwaitingConsumer<>();
    context.evaluate(task).onFail(eval);

    // should not complete before we unblock the blocked promise
    assertThat(eval.await(1, TimeUnit.SECONDS), is(false));

    blocked.awaitAndGet().set(42);
    Throwable throwable = eval.awaitAndGet();

    assertThat(throwable, instanceOf(RuntimeException.class));
    assertThat(throwable.getMessage(), is("failed"));
  }
}
