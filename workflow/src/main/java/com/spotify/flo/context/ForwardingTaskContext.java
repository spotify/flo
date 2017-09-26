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

package com.spotify.flo.context;

import com.spotify.flo.Fn;
import com.spotify.flo.Task;
import com.spotify.flo.TaskContext;
import com.spotify.flo.TaskId;
import java.util.Objects;

/**
 * A {@link TaskContext} that forwards calls.
 */
public abstract class ForwardingTaskContext implements TaskContext {

  protected final TaskContext delegate;

  protected ForwardingTaskContext(TaskContext delegate) {
    this.delegate = Objects.requireNonNull(delegate);
  }

  @Override
  public <T> Value<T> evaluateInternal(Task<T> task, TaskContext context) {
    return delegate.evaluateInternal(task, context);
  }

  @Override
  public <T> Value<T> invokeProcessFn(TaskId taskId, Fn<Value<T>> processFn) {
    return delegate.invokeProcessFn(taskId, processFn);
  }

  @Override
  public <T> Value<T> value(Fn<T> value) {
    return delegate.value(value);
  }

  @Override
  public <T> Value<T> immediateValue(T value) {
    return delegate.immediateValue(value);
  }

  @Override
  public <T> Promise<T> promise() {
    return delegate.promise();
  }
}
