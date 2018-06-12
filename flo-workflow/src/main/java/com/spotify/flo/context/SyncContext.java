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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;

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
    return new ValuePromise<>();
  }

  private final class DirectValue<T> implements Value<T> {

    private final Semaphore setLatch;
    private final List<Consumer<T>> valueConsumers = new ArrayList<>();
    private final List<Consumer<Throwable>> failureConsumers = new ArrayList<>();
    private final AtomicReference<Consumer<Consumer<T>>> valueReceiver;
    private final AtomicReference<Consumer<Consumer<Throwable>>> failureReceiver;

    private DirectValue() {
      valueReceiver = new AtomicReference<>(valueConsumers::add);
      failureReceiver = new AtomicReference<>(failureConsumers::add);
      this.setLatch = new Semaphore(1);
    }

    @Override
    public EvalContext context() {
      return SyncContext.this;
    }

    @Override
    public <U> Value<U> flatMap(Function<? super T, ? extends Value<? extends U>> fn) {
      Promise<U> promise = promise();
      consume(t -> {
        final Value<? extends U> apply = fn.apply(t);
        apply.consume(promise::set);
        apply.onFail(promise::fail);
      });
      onFail(promise::fail);
      return promise.value();
    }

    @Override
    public void consume(Consumer<T> consumer) {
      valueReceiver.get().accept(consumer);
    }

    @Override
    public void onFail(Consumer<Throwable> errorConsumer) {
      failureReceiver.get().accept(errorConsumer);
    }
  }

  private final class ValuePromise<T> implements Promise<T> {

    private final DirectValue<T> value = new DirectValue<>();

    @Override
    public Value<T> value() {
      return value;
    }

    @Override
    public void set(T t) {
      final boolean completed = value.setLatch.tryAcquire();
      if (!completed) {
        throw new IllegalStateException("Promise was already completed");
      } else {
        value.valueReceiver.set(c -> c.accept(t));
        value.valueConsumers.forEach(c -> c.accept(t));
      }
    }

    @Override
    public void fail(Throwable throwable) {
      final boolean completed = value.setLatch.tryAcquire();
      if (!completed) {
        throw new IllegalStateException("Promise was already completed");
      } else {
        value.failureReceiver.set(c -> c.accept(throwable));
        value.failureConsumers.forEach(c -> c.accept(throwable));
      }
    }
  }
}
