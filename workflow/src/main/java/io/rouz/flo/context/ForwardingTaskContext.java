package io.rouz.flo.context;

import java.util.List;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.stream.Collector;

import io.rouz.flo.Fn;
import io.rouz.flo.Task;
import io.rouz.flo.TaskContext;
import io.rouz.flo.TaskId;

/**
 * A {@link TaskContext} that forwards calls.
 */
public abstract class ForwardingTaskContext implements TaskContext {

  protected final TaskContext delegate;

  protected ForwardingTaskContext(TaskContext delegate) {
    this.delegate = Objects.requireNonNull(delegate);
  }

  @Override
  public <T> Value<T> evaluateInternal(Task<T> task, TaskContext context) {
    return delegate.evaluateInternal(task, context);
  }

  @Override
  public <T> Value<T> invokeProcessFn(TaskId taskId, Fn<Value<T>> processFn) {
    return delegate.invokeProcessFn(taskId, processFn);
  }

  @Override
  public <T> Value<T> value(Fn<T> value) {
    return delegate.value(value);
  }

  @Override
  public <T> Value<T> immediateValue(T value) {
    return delegate.immediateValue(value);
  }

  @Override
  public <T> Promise<T> promise() {
    return delegate.promise();
  }

  @Override
  public <T, U, V> Value<V> mapBoth(
      Value<T> first,
      Value<U> second,
      BiFunction<? super T, ? super U, ? extends V> fn) {
    return delegate.mapBoth(first, second, fn);
  }

  @Override
  public <T> Collector<Value<T>, ?, Value<List<T>>> toValueList() {
    return delegate.toValueList();
  }
}
