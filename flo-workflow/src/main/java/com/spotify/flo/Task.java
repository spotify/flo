/*-
 * -\-\-
 * Flo Workflow Definition
 * --
 * Copyright (C) 2016 - 2017 Spotify AB
 * --
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * -/-/-
 */

package com.spotify.flo;

import com.google.auto.value.AutoValue;
import java.io.Serializable;
import java.util.Collections;
import java.util.List;

/**
 * A task is some work that has to be done, once a list of input tasks have completed. Tasks are
 * constructed using a {@link TaskBuilder}, use {@link Task#named(String, Object...)} as the
 * starting point.
 *
 * <p>Tasks can not be directly evaluated. Instead, a {@link EvalContext} has to be used. The
 * context will determine further details about the evaluation (threading, memoization,
 * instrumentation, etc.). See {@link EvalContext#evaluate(Task)}.
 *
 * @param <T>  A type carrying the execution metadata of this task
 */
@AutoValue
public abstract class Task<T> implements Serializable {

  private static final long serialVersionUID = 1L;

  public abstract TaskId id();

  public abstract Class<T> type();

  abstract Fn<List<Task<?>>> lazyInputs();

  public abstract List<TaskContext<?, ? super T>> contexts();

  abstract Invokable processFn();

  abstract List<ProcessFnArg> args();

  public List<Task<?>> inputs() {
    return lazyInputs().get();
  }

  public static NamedTaskBuilder named(String taskName, Object... args) {
    return new NTB(TaskId.create(taskName, args));
  }

  public static <T> Task<T> create(Fn<T> code, Class<T> type, String taskName, Object... args) {
    return create(
        Collections::emptyList, Collections.emptyList(),
        type,
        TaskId.create(taskName, args),
        a -> code.get(),
        Collections.emptyList());
  }

  static <T> Task<T> create(
      Fn<List<Task<?>>> inputs,
      List<TaskContext<?, ? super T>> contexts,
      Class<T> type,
      TaskId taskId,
      Invokable processFn,
      List<ProcessFnArg> args) {
    if (contexts.stream().filter(c -> c instanceof TaskOutput).count() > 1) {
      throw new IllegalArgumentException("A task can have at most one TaskOutput");
    }
    if (contexts.stream().filter(c -> c instanceof TaskOperator).count() > 1) {
      throw new IllegalArgumentException("A task can have at most one TaskOperator");
    }
    final AutoValue_Task<T> task = new AutoValue_Task<>(taskId, type, inputs, contexts, processFn, args);
    return Serialization.requireSerializable(task, "task");
  }

  /**
   * This is only needed because a generic lambda can not be implemented
   * in {@link #named(String, Object...)}.
   */
  private static final class NTB implements NamedTaskBuilder {

    private final TaskId taskId;

    private NTB(TaskId taskId) {
      this.taskId = taskId;
    }

    @Override
    public <Z> TaskBuilder<Z> ofType(Class<Z> type) {
      return TaskBuilderImpl.rootBuilder(taskId, type);
    }
  }
}
