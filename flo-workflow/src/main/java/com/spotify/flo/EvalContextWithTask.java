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

import static java.util.Objects.requireNonNull;

import com.spotify.flo.context.ForwardingEvalContext;
import java.util.Optional;

/**
 * A {@link EvalContext} that overrides {@link EvalContext#currentTask()} to return a specific {@link TaskId}.
 */
public class EvalContextWithTask extends ForwardingEvalContext {

  private final Task<?> task;

  private EvalContextWithTask(EvalContext delegate, Task<?> task) {
    super(delegate);
    this.task = requireNonNull(task);
  }

  static EvalContext withTask(EvalContext delegate, Task<?> task) {
    return new EvalContextWithTask(delegate, task);
  }

  @Override
  public Optional<Task<?>> currentTask() {
    return Optional.of(task);
  }
}
