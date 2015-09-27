package io.rouz.task;

/**
 * A context for task evaluation
 */
interface TaskContext {

  boolean has(TaskId taskId);
  <V> V value(TaskId taskId);
  <V> void put(TaskId taskId, V value);
}
