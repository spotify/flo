package io.rouz.flo;

import static java.util.Objects.requireNonNull;

import io.rouz.flo.context.ForwardingTaskContext;
import java.util.Optional;

/**
 * A {@link TaskContext} that overrides {@link TaskContext#currentTask()} to return a specific {@link TaskId}.
 */
public class TaskContextWithTask extends ForwardingTaskContext {

  private final Task<?> task;

  private TaskContextWithTask(TaskContext delegate, Task<?> task) {
    super(delegate);
    this.task = requireNonNull(task);
  }

  static TaskContext withTask(TaskContext delegate, Task<?> task) {
    return new TaskContextWithTask(delegate, task);
  }

  @Override
  public Optional<Task<?>> currentTask() {
    return Optional.of(task);
  }
}
