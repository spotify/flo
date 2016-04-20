package io.rouz.task.dsl;

import io.rouz.task.Task;

/**
 * The initial part of the {@link TaskBuilder} api which only holds the task name.
 * See {@link Task#named(String, Object...)}.
 */
public interface NamedTaskBuilder {
  <Z> TaskBuilder<Z> ofType(Class<Z> type);
}
