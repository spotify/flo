/*-
 * -\-\-
 * Flo Task Execute Scio
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

package com.spotify.flo.execute.scio;

import com.spotify.flo.EvalContext;
import com.spotify.flo.Fn;
import com.spotify.flo.Invokable;
import com.spotify.flo.Task;
import com.spotify.flo.TaskBridge;
import com.spotify.flo.TaskId;
import com.spotify.flo.freezer.Persisted;

class ScioJobSpecExtractingContext implements EvalContext {

  private final TaskId taskId;

  public ScioJobSpecExtractingContext(TaskId taskId, CompletableFuture<ScioJobSpec> specFuture) {
    this.taskId = taskId;
  }

  @Override
  public <T> Value<T> value(Fn<T> value) {
    return EvalContext.sync().value(value);
  }

  @Override
  public <T> Promise<T> promise() {
    return EvalContext.sync().promise();
  }

  @Override
  public <T> Value<T> evaluateInternal(Task<T> task, EvalContext context) {

    if (!taskId.equals(task.id())) {
      throw new AssertionError("Unexpected task: " + task);
    }

    final Object[] args = TaskBridge.args(task).stream()
        .map(arg -> arg.get(context))
        .map(value -> value.map(v -> (Object) v))
        .map(Value::get)
        .toArray(Object[]::new);

    final Invokable processFn = TaskBridge.processFn(task);

    final Promise<T> promise = promise();
    promise.fail(new Persisted());

    final ScioJobSpec spec = processFn.invoke(args);
  }
}
