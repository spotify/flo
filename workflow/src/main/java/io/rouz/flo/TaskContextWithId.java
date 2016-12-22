package io.rouz.flo;

import java.util.Optional;

import io.rouz.flo.context.ForwardingTaskContext;

import static java.util.Objects.requireNonNull;

/**
 * A {@link TaskContext} that overrides {@link #currentTaskId()} to return a specific {@link TaskId}.
 */
class TaskContextWithId extends ForwardingTaskContext {

  private final TaskId taskId;

  private TaskContextWithId(TaskContext delegate, TaskId taskId) {
    super(delegate);
    this.taskId = requireNonNull(taskId);
  }

  static TaskContext withId(TaskContext delegate, TaskId taskId) {
    return new TaskContextWithId(delegate, taskId);
  }

  @Override
  public Optional<TaskId> currentTaskId() {
    return Optional.of(taskId);
  }
}
