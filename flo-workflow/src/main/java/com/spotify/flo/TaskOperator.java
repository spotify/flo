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

import com.spotify.flo.context.OperatingContext;
import java.io.Serializable;

/**
 * An operator controls the execution of a job for a task,  e.g. a data processing job on some processing platform.
 *
 * <p>The concrete operator implementation should {@link #provide(EvalContext)} the task with some means of constructing
 * an operation description. The operation description should be thrown out of the process fn using an
 * {@link OperationException} to be caught and executed by the {@link OperatingContext}.
 */
public abstract class TaskOperator<T> extends TaskContextGeneric<T> {

  public abstract static class OperationException extends ControlException {

    public abstract Operation operation();

    public abstract <T> T run(Listener listener);
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
  }
}
