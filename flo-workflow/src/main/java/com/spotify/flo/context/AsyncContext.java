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
import com.spotify.flo.Task;
import io.grpc.Context;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * A {@link EvalContext} that executes evaluation and {@link Value} computations on a given
 * {@link Executor}.
 *
 * <p>Override {@link #evaluate(Task)} to implement {@link Value} memoization.
 */
public class AsyncContext implements EvalContext {

  private final Executor executor;

  private AsyncContext(Executor executor) {
    this.executor = Context.currentContextExecutor(Objects.requireNonNull(executor));
  }

  public static EvalContext create(Executor executor) {
    return new AsyncContext(executor);
  }

  @Override
  public <T> Value<T> evaluateInternal(Task<T> task, EvalContext context) {
    final Promise<T> promise = promise();
    executor.execute(() -> {
      final Value<T> tValue = EvalContext.super.evaluateInternal(task, context);
      tValue.consume(promise::set);
      tValue.onFail(promise::fail);
    });
    return promise.value();
  }

  @Override
  public final <T> Value<T> value(Fn<T> t) {
    return new FuturePromise<>(this, executor, CompletableFuture.supplyAsync(t, executor))
        .value();
  }

  @Override
  public final <T> Value<T> immediateValue(T t) {
    return new FuturePromise<>(this, executor, CompletableFuture.completedFuture(t))
        .value();
  }

  @Override
  public <T> Promise<T> promise() {
    return new FuturePromise<>(this, executor, new CompletableFuture<>());
  }
}
