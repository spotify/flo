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

import com.spotify.flo.EvalContext.Value;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collector;
import java.util.stream.Stream;

/**
 * A function that folds a {@link List} of {@link Value}s into a {@link Value} of a {@link List}.
 *
 * <p>It can be used with a {@link Collector} to fold a {@link Stream} of {@link Value}s.
 */
final class ValueFold<T> implements Function<List<Value<T>>, Value<List<T>>> {

  private final EvalContext evalContext;

  ValueFold(EvalContext evalContext) {
    this.evalContext = Objects.requireNonNull(evalContext);
  }

  static <T> ValueFold<T> inContext(EvalContext evalContext) {
    return new ValueFold<>(evalContext);
  }

  @Override
  public Value<List<T>> apply(List<Value<T>> list) {
    Value<List<T>> values = evalContext.immediateValue(new ArrayList<>());
    for (Value<T> tValue : list) {
      values = Values.mapBoth(evalContext, values, tValue, (l, t) -> {
        l.add(t);
        return l;
      });
    }
    return values;
  }
}
