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
import com.spotify.flo.TaskBuilder.F0;
import com.spotify.flo.TaskContext;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * A {@link TaskContext} that executes evaluation and {@link Value} computations on a given
 * {@link Executor}.
 *
 * <p>Override {@link #evaluate(Task)} to implement {@link Value} memoization.
 */
public class AsyncContext implements TaskContext {

  private final Executor executor;

  protected AsyncContext(Executor executor) {
    this.executor = Objects.requireNonNull(executor);
  }

  public static TaskContext create(Executor executor) {
    return new AsyncContext(executor);
  }

  @Override
  public <T> Value<T> evaluateInternal(Task<T> task, TaskContext context) {
    return flatten(() -> TaskContext.super.evaluateInternal(task, context));
  }

  @Override
  public final <T> Value<T> value(Fn<T> t) {
    return new FutureValue<>(CompletableFuture.supplyAsync(t, executor));
  }

  @Override
  public final <T> Value<T> immediateValue(T t) {
    return new FutureValue<>(CompletableFuture.completedFuture(t));
  }

  @Override
  public <T> Promise<T> promise() {
    return new FuturePromise<>();
  }

  protected final <T> Value<T> flatten(F0<Value<T>> t) {
    return flatten(CompletableFuture.supplyAsync(t, executor));
  }

  protected final <T> Value<T> flatten(CompletionStage<? extends Value<? extends T>> future) {
    final CompletableFuture<T> next = new CompletableFuture<>();
    future.whenCompleteAsync(
        (value, throwable) -> {
          if (throwable != null) {
            next.completeExceptionally(resolveAppThrowable(throwable));
          } else {
            value.consume(next::complete);
            value.onFail(next::completeExceptionally);
          }
        },
        executor);
    return new FutureValue<>(next);
  }

  private static Throwable resolveAppThrowable(Throwable throwable) {
    return (throwable instanceof CompletionException)
        ? resolveAppThrowable(throwable.getCause())
        : throwable;
  }

  private final class FutureValue<T> implements Value<T> {

    private final CompletionStage<T> future;

    private FutureValue(CompletionStage<T> future) {
      this.future = future;
    }

    @Override
    public TaskContext context() {
      return AsyncContext.this;
    }

    @Override
    public void consume(Consumer<T> consumer) {
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
    public <U> Value<U> map(Function<? super T, ? extends U> fn) {
      return new FutureValue<>(future.thenApplyAsync(fn, executor));
    }

    @Override
    public <U> Value<U> flatMap(Function<? super T, ? extends Value<? extends U>> function) {
      return flatten(future.thenApplyAsync(function, executor));
    }
  }

  private final class FuturePromise<T> implements Promise<T> {

    private final CompletableFuture<T> future = new CompletableFuture<>();
    private final FutureValue<T> value = new FutureValue<>(future);

    @Override
    public Value<T> value() {
      return value;
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
  }
}
