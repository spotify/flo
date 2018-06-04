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

package com.spotify.flo.context;

import static com.spotify.flo.Tracing.TASK_ID;

import com.spotify.flo.EvalContext;
import com.spotify.flo.Fn;
import com.spotify.flo.Task;
import com.spotify.flo.TaskId;
import io.grpc.Context;

/**
 * A {@link EvalContext} that sets the current {@link Task#id()} on the {@link Context} when calling the process fn.
 */
public class TracingContext extends ForwardingEvalContext {

  private TracingContext(EvalContext delegate) {
    super(delegate);
  }

  public static EvalContext composeWith(EvalContext baseContext) {
    return new TracingContext(baseContext);
  }

  @Override
  public <T> Value<T> invokeProcessFn(TaskId taskId, Fn<Value<T>> processFn) {
    try {
      return Context.current()
          .withValue(TASK_ID, taskId)
          .call(() -> super.invokeProcessFn(taskId, processFn));
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
