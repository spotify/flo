package io.rouz.task;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Not thread safe, use with one thread only
 */
class TaskContextImpl implements TaskContext {

  private Map<TaskId, Object> cache =  new HashMap<>();

  @Override
  public boolean has(TaskId taskId) {
    return cache.containsKey(taskId);
  }

  @Override
  public <V> V value(TaskId taskId) {
    //noinspection unchecked
    return (V) cache.get(taskId);
  }

  @Override
  public <V> void put(TaskId taskId, V value) {
    cache.put(taskId, value);
  }

  @Override
  public <T> Value<T> value(T value) {
    return new ImmediateValue<>(value);
  }

  private class ImmediateValue<T> implements Value<T> {

    private final T value;

    private ImmediateValue(T value) {
      this.value = Objects.requireNonNull(value);
    }

    @Override
    public TaskContext context() {
      return TaskContextImpl.this;
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
