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

import java.io.Serializable;

/**
 * An operator controls the execution of a job for a task,  e.g. a data processing job on some processing platform.
 *
 * <p>The concrete operator implementation should {@link #provide(EvalContext)} the task with some means of constructing
 * an operation description. The operation description should be returned from the process fn.
 */
public abstract class TaskOperator<T, Y extends TaskOperator.OperatorSpec<Z>, Z> implements TaskContext<T> {

  public interface OperatorSpec<ZZ> {
    ZZ run(Listener listener);
  }

  public interface Listener extends Serializable {

    /**
     * Called to report some piece of task metadata.
     *
     * @param task The task that is being evaluated
     * @param key The metadata key.
     * @param value The metadata value.
     */
    void meta(TaskId task, String key, String value);

    Listener NOP = (Listener) (task, key, value) -> { };

    default Listener composeWith(Listener listener) {
      return (task, key, value) -> {
        meta(task, key, value);
        listener.meta(task, key, value);
      };
    }
  }

  public abstract Z perform(Y o, Listener listener);
}
