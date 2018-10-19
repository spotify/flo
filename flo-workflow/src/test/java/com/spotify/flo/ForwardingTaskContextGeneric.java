/*-
 * -\-\-
 * Flo Workflow Definition
 * --
 * Copyright (C) 2016 - 2018 Spotify AB
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

public class ForwardingTaskContextGeneric<T> extends TaskContextGeneric<T> {
  private final Fn<TaskContextGeneric<T>> delegate;

  private ForwardingTaskContextGeneric(Fn<TaskContextGeneric<T>> delegate) {
    this.delegate = delegate;
  }

  @Override
  public T provide(EvalContext evalContext) {
    return delegate.get().provide(evalContext);
  }

  @Override
  public void preRun(Task<?> task) {
    delegate.get().preRun(task);
  }

  @Override
  public void onSuccess(Task<?> task, Object z) {
    delegate.get().onSuccess(task, z);
  }

  @Override
  public void onFail(Task<?> task, Throwable throwable) {
    delegate.get().onFail(task, throwable);
  }

  public static <T> TaskContextGeneric<T> forwardingContext(Fn<TaskContextGeneric<T>> delegate) {
    return new ForwardingTaskContextGeneric<>(delegate);
  }
}
