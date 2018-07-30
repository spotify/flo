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

import com.spotify.flo.EvalContext;
import com.spotify.flo.Fn;
import com.spotify.flo.TaskId;
import com.spotify.flo.TaskOperator.OperationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An {@link EvalContext} that may return a value without calling the processFn or evaluating its
 * dependencies.
 */
public class OperatingContext extends ForwardingEvalContext {

  private static final Logger LOG = LoggerFactory.getLogger(OperatingContext.class);

  private OperatingContext(EvalContext delegate) {
    super(delegate);
  }

  public static EvalContext composeWith(EvalContext baseContext) {
    return new OperatingContext(baseContext);
  }

  @Override
  public <T> Value<T> invokeProcessFn(TaskId taskId, Fn<T> processFn) {
    return super.invokeProcessFn(taskId, () -> {
      try {
        return processFn.get();
      } catch (OperationException e) {
        return e.run();
      }
    });
  }
}
