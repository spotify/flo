package io.rouz.task;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collector;

import static java.util.Objects.requireNonNull;

/**
 * A {@link TaskContext} that overrides {@link #currentTaskId()} to return a specific {@link TaskId}.
 */
class TaskContextWithId implements TaskContext {

  private final TaskContext delegate;
  private final TaskId taskId;

  private TaskContextWithId(TaskContext delegate, TaskId taskId) {
    this.delegate = requireNonNull(delegate);
    this.taskId = requireNonNull(taskId);
  }

  static TaskContext withId(TaskContext delegate, TaskId taskId) {
    return new TaskContextWithId(delegate, taskId);
  }

  @Override
  public Optional<TaskId> currentTaskId() {
    return Optional.of(taskId);
  }

  // === forwarding methods =======================================================================

  @Override
  public <T> Value<T> evaluate(Task<T> task) {
    return delegate.evaluate(task);
  }

  @Override
  public <T> Value<T> invokeProcessFn(TaskId taskId, TaskBuilder.F0<Value<T>> processFn) {
    return delegate.invokeProcessFn(taskId, processFn);
  }

  @Override
  public <T> Value<T> value(TaskBuilder.F0<T> value) {
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
  public <T> Collector<Value<T>, ?, Value<List<T>>> toValueList() {
    return delegate.toValueList();
  }
}
