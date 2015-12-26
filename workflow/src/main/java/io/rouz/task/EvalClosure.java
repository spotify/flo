package io.rouz.task;

import java.io.Serializable;

import io.rouz.task.TaskContext.Value;

/**
 * A function that evaluates some {@link Value} in a given {@link TaskContext}
 */
@FunctionalInterface
interface EvalClosure<T> extends Serializable {

  /**
   * Produces a {@link Value} in a given {@link TaskContext}.
   *
   * The produced {@link Value} should follow the semantics defined by the used {@link TaskContext}.
   *
   * @param taskContext  The context to evaluate in
   * @return a value
   */
  Value<T> eval(TaskContext taskContext);
}
