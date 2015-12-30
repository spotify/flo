package io.rouz.task.context;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Function;

import io.rouz.task.Task;
import io.rouz.task.TaskContext;
import io.rouz.task.TaskId;
import io.rouz.task.dsl.TaskBuilder.F0;

/**
 * A {@link TaskContext} that evaluates tasks immediately and memoizes results in memory.
 *
 * Memoized results are tied to the instance the evaluated the values.
 */
public class InMemImmediateContext implements TaskContext {

  private Map<TaskId, Object> cache =  new HashMap<>();

  private InMemImmediateContext() {
  }

  public static TaskContext create() {
    return new InMemImmediateContext();
  }

  @Override
  public <T> Value<T> evaluate(Task<T> task) {
    final TaskId taskId =  task.id();

    final Value<T> value;
    if (has(taskId)) {
      value = get(taskId);
      LOG.debug("Found calculated value for {} = {}", taskId, value);
    } else {
      value = TaskContext.super.evaluate(task);
      put(taskId, value);
    }

    return value;
  }

  @Override
  public <T> Value<T> value(F0<T> value) {
    return new ImmediateValue<>(value.get());
  }

  private boolean has(TaskId taskId) {
    return cache.containsKey(taskId);
  }

  private <V> void put(TaskId taskId, V value) {
    cache.put(taskId, value);
  }

  private <V> V get(TaskId taskId) {
    //noinspection unchecked
    return (V) cache.get(taskId);
  }

  private class ImmediateValue<T> implements Value<T> {

    private final T value;

    private ImmediateValue(T value) {
      this.value = Objects.requireNonNull(value);
    }

    @Override
    public TaskContext context() {
      return InMemImmediateContext.this;
    }

    @Override
    public <U> Value<U> flatMap(Function<? super T, ? extends Value<? extends U>> fn) {
      //noinspection unchecked
      return (Value<U>) fn.apply(value);
    }

    @Override
    public void consume(Consumer<T> consumer) {
      consumer.accept(value);
    }
  }
}
