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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * A {@link EvalContext} that evaluates tasks immediately.
 *
 * <p>This context is not thread safe.
 */
public class SyncContext implements EvalContext {

  private SyncContext() {
  }

  public static EvalContext create() {
    return new SyncContext();
  }

  @Override
  public <T> Value<T> value(Fn<T> value) {
    final Promise<T> promise = promise();

    try {
      promise.set(value.get());
    } catch (Throwable t) {
      promise.fail(t);
    }

    return promise.value();
  }

  @Override
  public <T> Promise<T> promise() {
    return new FuturePromise<>(this, new SameThreadExecutor(), new CompletableFuture<>());
  }

  private static class SameThreadExecutor implements Executor {

    @Override
    public void execute(Runnable command) {
      command.run();
    }
  }
}
