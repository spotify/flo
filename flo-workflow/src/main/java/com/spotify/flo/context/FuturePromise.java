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

import com.spotify.flo.EvalContext;
import com.spotify.flo.EvalContext.Promise;
import com.spotify.flo.EvalContext.Value;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.function.Consumer;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;

final class FuturePromise<T> implements Promise<T> {

  private final EvalContext evalContext;
  private final Executor executor;

  private final CompletableFuture<T> future;

  FuturePromise(EvalContext evalContext, Executor executor, CompletableFuture<T> future) {
    this.evalContext = requireNonNull(evalContext);
    this.executor = requireNonNull(executor);
    this.future = requireNonNull(future);
  }

  @Override
  public Value<T> value() {
    return new FutureValue<>(future);
  }

  @Override
  public void set(T t) {
    final boolean completed = future.complete(t);
    if (!completed) {
      throw new IllegalStateException("Promise was already completed");
    }
  }

  @Override
  public void fail(Throwable throwable) {
    final boolean completed = future.completeExceptionally(throwable);
    if (!completed) {
      throw new IllegalStateException("Promise was already completed");
    }
  }

  private static Throwable resolveAppThrowable(Throwable throwable) {
    return (throwable instanceof CompletionException)
        ? resolveAppThrowable(throwable.getCause())
        : throwable;
  }

  class FutureValue<V> implements Value<V> {

    private final CompletionStage<V> future;

    FutureValue(CompletionStage<V> future) {
      this.future = requireNonNull(future);
    }

    @Override
    public EvalContext context() {
      return evalContext;
    }

    @Override
    public void consume(Consumer<V> consumer) {
      future.thenAcceptAsync(consumer, executor);
    }

    @Override
    public void onFail(Consumer<Throwable> errorConsumer) {
      future.whenCompleteAsync((ˍ, throwable) -> {
        if (throwable != null) {
          errorConsumer.accept(resolveAppThrowable(throwable));
        }
      }, executor);
    }

    @Override
    public <U> Value<U> map(Function<? super V, ? extends U> fn) {
      return new FutureValue<>(future.thenApplyAsync(fn, executor));
    }

    @Override
    public <U> Value<U> flatMap(Function<? super V, ? extends Value<? extends U>> function) {
      final CompletableFuture<U> next = new CompletableFuture<>();
      CompletionStage<? extends Value<? extends U>> mapped =
          future.thenApplyAsync(function, executor);

      mapped.whenCompleteAsync(
          (value, throwable) -> {
            if (throwable != null) {
              next.completeExceptionally(resolveAppThrowable(throwable));
            } else {
              value.consume(next::complete);
              value.onFail(next::completeExceptionally);
            }
          },
          executor);
      return new FuturePromise<>(evalContext, executor, next).value();
    }
  }
}
