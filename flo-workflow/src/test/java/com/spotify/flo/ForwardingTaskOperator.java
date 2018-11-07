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

public class ForwardingTaskOperator<ContextT, SpecT, ResultT> implements TaskOperator<ContextT, SpecT, ResultT> {

  private final Fn<TaskOperator<ContextT, SpecT, ResultT>> delegate;

  private ForwardingTaskOperator(Fn<TaskOperator<ContextT, SpecT, ResultT>> delegate) {
    this.delegate = delegate;
  }

  @Override
  public ResultT perform(SpecT spec, Listener listener) {
    return delegate.get().perform(spec, listener);
  }

  @Override
  public ContextT provide(EvalContext evalContext) {
    return delegate.get().provide(evalContext);
  }

  public static <ContextT, SpecT, ResultT> TaskOperator<ContextT, SpecT, ResultT> forwardingOperator(
      Fn<TaskOperator<ContextT, SpecT, ResultT>> delegate) {
    return new ForwardingTaskOperator<>(delegate);
  }
}
