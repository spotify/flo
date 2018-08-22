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
import java.util.Map;

/**
 * An operator controls the execution of a job for a task,  e.g. a data processing job on some processing platform.
 *
 * <p>The concrete operator implementation should {@link #provide(EvalContext)} the task with some means of constructing
 * an operation description. The operation description should be returned from the process fn.
 */
public interface TaskOperator<ContextT, SpecT, ResultT>
    extends TaskContext<ContextT> {

  ResultT perform(SpecT spec, Listener listener);

  interface Listener extends Serializable {

    /**
     * Called to report some piece of task metadata.
     *
     * @param task The task that is being evaluated
     * @param key The metadata key.
     * @param value The metadata value.
     */
    void meta(TaskId task, String key, String value);

    /**
     * Called to report some piece of task metadata.
     *
     * By default this methods calls <code>meta(TaskId task, String key, String value)</code>
     * for each key-value pair.
     *
     * @param task The task that is being evaluated
     * @param data The key-value metadata
     */
    default void meta(TaskId task, Map<String, String> data) {
      data.forEach((key, value) -> meta(task, key, value));
    }

    Listener NOP = (Listener) (task, key, value) -> { };

    default Listener composeWith(Listener listener) {
      return new Listener() {
        @Override
        public void meta(TaskId task, String key, String value) {
          Listener.this.meta(task, key, value);
          listener.meta(task, key, value);
        }

        @Override
        public void meta(TaskId task, Map<String, String> data) {
          Listener.this.meta(task, data);
          listener.meta(task, data);
        }
      };
    }
  }
}
