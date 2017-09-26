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

package com.spotify.flo;

import com.spotify.flo.TaskContext.Value;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.stream.Collector;
import java.util.stream.Stream;

/**
 * Utilities for manipulating instances of {@link Value}.
 */
public final class Values {

  private Values() {
  }

  /**
   * Maps a function over two {@link Value}s returning a new {@link Value} of the result which
   * only becomes available when both inputs have completed.
   *
   * <p>The returned {@link Value} will not complete until both input values have completed either
   * successfully or with an exception. If both inputs fail with an exception, the exception from
   * {@code first} will be propagated into the returned value.
   *
   * @param context The context which values are processed in
   * @param first   The first input value
   * @param second  The second input value
   * @param fn      The map function
   * @param <T>     The type of the first input value
   * @param <U>     The type of the second input value
   * @param <V>     The type of the return value
   * @return A value that completes only when both inputs have completed
   */
  public static <T, U, V> Value<V> mapBoth(
      TaskContext context,
      Value<T> first,
      Value<U> second,
      BiFunction<? super T, ? super U, ? extends V> fn) {
    TaskContext.Promise<V> promise = context.promise();

    BiConsumer<T, Throwable> firstComplete = (t, firstThrowable) -> {
      BiConsumer<U, Throwable> secondComplete = (v, secondThrowable) -> {
        if (firstThrowable != null) {
          promise.fail(firstThrowable);
        } else if (secondThrowable != null) {
          promise.fail(secondThrowable);
        } else {
          promise.set(fn.apply(t, v));
        }
      };

      second.consume(v -> secondComplete.accept(v, null));
      second.onFail(e -> secondComplete.accept(null, e));
    };

    first.consume(t -> firstComplete.accept(t, null));
    first.onFail(e -> firstComplete.accept(null, e));

    return promise.value();
  }

  /**
   * A {@link Collector} that collects a {@link Stream} of {@link Value}s into a {@link Value}
   * of a {@link List}.
   *
   * <p>The semantics of joining {@link Value}s is decided by this {@link TaskContext}.
   *
   * @param context The context which values are processed in
   * @param <T>     The inner type of the values
   * @return A collector for a stream of values
   */
  public static <T> Collector<Value<T>, ?, Value<List<T>>> toValueList(TaskContext context) {
    return Collector.of(
        ArrayList::new, List::add, (a, b) -> { a.addAll(b); return a; },
        ValueFold.inContext(context));
  }
}
