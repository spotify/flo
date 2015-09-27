package io.rouz.task;

import java.util.HashMap;
import java.util.Map;

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
}
