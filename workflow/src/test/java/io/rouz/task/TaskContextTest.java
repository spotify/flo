package io.rouz.task;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;

import io.rouz.task.dsl.TaskBuilder;

import static org.hamcrest.CoreMatchers.startsWith;
import static org.junit.Assert.assertThat;

public class TaskContextTest {

  @Test
  public void shouldEvaluate() throws Exception {
    Task<String> foo = rep("foo");

    FakeContext fakeContext = new FakeContext();
    fakeContext.evaluate(foo);

    assertThat(fakeContext.ids.get(0).toString(), startsWith("Rep(foo)"));
    assertThat(fakeContext.ids.get(1).toString(), startsWith("Rep(oo)"));
    assertThat(fakeContext.ids.get(2).toString(), startsWith("Rep(o)"));
  }

  Task<String> rep(String in) {
    TaskBuilder builder = Task.named("Rep", in);
    if (in.length() == 1) {
      return builder.constant(() -> in);
    }

    return builder
        .in(() -> rep(in.substring(1)))
        .process(x -> x + in);
  }

  private static class FakeContext implements TaskContext {

    List<TaskId> ids = new ArrayList<>();

    @Override
    public <T> Value<T> evaluate(Task<T> task) {
      ids.add(task.id());
      return TaskContext.super.evaluate(task);
    }

    @Override
    public boolean has(TaskId taskId) {
      return false;
    }

    @Override
    public <V> V value(TaskId taskId) {
      throw new UnsupportedOperationException("Should not happen");
    }

    @Override
    public <V> void put(TaskId taskId, V value) {
      // noop
    }

    @Override
    public <T> Value<T> value(T value) {
      return new SyncValue<>(value);
    }

    private class SyncValue<T> implements Value<T> {

      private final T t;

      public SyncValue(T t) {
        this.t = t;
      }

      @Override
      public TaskContext context() {
        return FakeContext.this;
      }

      @Override
      public <U> Value<U> flatMap(Function<? super T, ? extends Value<? extends U>> fn) {
        //noinspection unchecked
        return (Value<U>) fn.apply(t);
      }

      @Override
      public void consume(Consumer<T> consumer) {
        consumer.accept(t);
      }
    }
  }
}
