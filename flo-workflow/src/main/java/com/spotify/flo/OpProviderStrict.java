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

import static java.util.Optional.empty;

import java.util.Optional;

/**
 * It extends the {@link OpProvider} interface to make the operator aware of the expected return
 * type of the task. This makes the operator less generic but it allows to override results and
 * skip the task evaluation as well as utilizing the returned value in case of successful task
 * execution.
 */
public interface OpProviderStrict<T, S> extends OpProvider<T> {

  /**
   * When a non empty value is returned, the {@link TaskContext} will not evaluate the task or its
   * upstreams but the returned value is used as task's result.
   *
   * @param taskContext The task context in which the current task is being evaluated
   * @return The optional result to be returned
   */
  default Optional<S> overrideResult(TaskContext taskContext) {
    return empty();
  }

  /**
   * Will be called just after a task that is using this operator has successfully evaluated.
   *
   * @param task The task that evaluated
   * @param z    The return value of the evaluated task
   */
  default void onSuccess(Task<?> task, S z) {
  }
}
