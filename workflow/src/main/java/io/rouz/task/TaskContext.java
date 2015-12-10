package io.rouz.task;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.rouz.task.dsl.TaskBuilder.F1;

/**
 * A context for task evaluation
 */
interface TaskContext {

  Logger LOG = LoggerFactory.getLogger(TaskContext.class);

  boolean has(TaskId taskId);
  <V> V value(TaskId taskId);
  <V> void put(TaskId taskId, V value);

  default <T> T apply(TaskId taskId, F1<TaskContext, T> code) {
    final T value;
    if (has(taskId)) {
      value = value(taskId);
      LOG.debug("Found calculated value for {} = {}", taskId, value);
    } else {
      value = code.apply(this);
      put(taskId, value);
    }
    return value;
  }
}
