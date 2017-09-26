/*-
 * -\-\-
 * flo runner
 * --
 * Copyright (C) 2016 Spotify AB
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

package com.spotify.flo.util;

import com.spotify.flo.TaskBuilder.F0;
import com.spotify.flo.TaskBuilder.F1;
import com.spotify.flo.TaskBuilder.F2;
import com.spotify.flo.TaskBuilder.F3;
import com.spotify.flo.TaskContext;
import com.spotify.flo.TaskContext.Value;

/**
 * experimental, do not use
 */
@FunctionalInterface
public interface ProcessingWrapper<Wrapped extends F1<TaskContext, Value<Output>>, Input, Output> {

  Wrapped create(F0<Input> input);

  default F1<TaskContext, Value<Output>> wrap(F0<Input> input) {
    return create(input);
  }

  default <A> F2<TaskContext, A, Value<Output>> wrap(F1<A, Input> input) {
    return (tc, a) -> create(() -> input.apply(a)).apply(tc);
  }

  default <A, B> F3<TaskContext, A, B, Value<Output>> warp(F2<A, B, Input> input) {
    return (tc, a, b) -> create(() -> input.apply(a, b)).apply(tc);
  }
}
