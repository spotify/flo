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
import com.spotify.flo.Task;
import com.spotify.flo.TaskId;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * A flo {@link EvalContext} that memoizes the results of task evaluations and
 * ensures that tasks are only evaluated once.
 */
public class MemoizingContext extends ForwardingEvalContext {

  private final ConcurrentMap<TaskId, Promise<?>> ongoing = new ConcurrentHashMap<>();

  private MemoizingContext(EvalContext baseContext) {
    super(baseContext);
  }

  public static EvalContext composeWith(EvalContext baseContext) {
    return new MemoizingContext(baseContext);
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T> Value<T> evaluateInternal(Task<T> task, EvalContext context) {
    // Unfortunately we cannot use computeIfAbsent here because modification of the CHM
    // in computeIfAbsents is not allowed: "... the computation should be short and simple,
    // and must not attempt to update any other mappings of this map.".
    // However, we could potentially use computeIfAbsent if we rewrite flo task evaluation
    // to be iterative instead of recursive.
    final Promise<T> promise = context.promise();
    final Promise<?> existing = ongoing.putIfAbsent(task.id(), promise);
    if (existing != null) {
      return (Value<T>) existing.value();
    }
    try {
      final Value<T> value = super.evaluateInternal(task, context);
      value.onFail(promise::fail);
      value.consume(promise::set);
    } catch (Throwable t) {
      promise.fail(t);
    }
    return promise.value();
  }
}
