package io.rouz.task;

import java.util.Collections;
import java.util.List;

/**
 * A convenience class for holding some reference. This is only so that we don't have to repeat
 * these declaration in every class above.
 */
class BaseRefs<Z> {

  final Fn<List<Task<?>>> inputs;
  final TaskId taskId;
  protected final Class<Z> type;

  BaseRefs(TaskId taskId, Class<Z> type) {
    this(Collections::emptyList, taskId, type);
  }

  BaseRefs(Fn<List<Task<?>>> inputs, TaskId taskId, Class<Z> type) {
    this.inputs = inputs;
    this.taskId = taskId;
    this.type = type;
  }
}
