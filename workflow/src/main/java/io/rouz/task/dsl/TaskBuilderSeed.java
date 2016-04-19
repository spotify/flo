package io.rouz.task.dsl;

import io.rouz.task.Task;

/**
 * The initial part of the {@link TaskBuilder} api which only holds the task
 * type argument {@link Z}. See {@link Task#ofType(Class)}.
 */
public interface TaskBuilderSeed<Z> {
  TaskBuilder<Z> named(String taskName, Object... args);
}
