package io.rouz.flo;

import io.rouz.flo.context.AwaitingConsumer;
import org.hamcrest.FeatureMatcher;
import org.hamcrest.Matcher;

public class TestUtils {

  public static <T> T evalAndGet(Task<T> task) throws InterruptedException {
    AwaitingConsumer<T> val = new AwaitingConsumer<>();
    TaskContext.inmem().evaluate(task).consume(val);
    return val.awaitAndGet();
  }

  public static Throwable evalAndGetException(Task<?> task) throws InterruptedException {
    AwaitingConsumer<Throwable> val = new AwaitingConsumer<>();
    TaskContext.inmem().evaluate(task).onFail(val);
    return val.awaitAndGet();
  }

  public static <T> Matcher<Task<? extends T>> taskId(Matcher<TaskId> taskIdMatcher) {
    return new FeatureMatcher<Task<? extends T>, TaskId>(taskIdMatcher, "Task with id", "taskId") {
      @Override
      protected TaskId featureValueOf(Task<? extends T> actual) {
        return actual.id();
      }
    };
  }
}
