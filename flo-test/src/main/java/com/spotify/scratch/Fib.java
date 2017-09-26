/*-
 * -\-\-
 * Flo Integration Tests
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

package com.spotify.scratch;

import com.spotify.flo.Task;
import com.spotify.flo.TaskBuilder;
import com.spotify.flo.TaskContext;
import com.spotify.flo.context.MemoizingContext;

/**
 * Simple fibonacci implementations
 */
final class Fib {

  public static void main(String[] args) {
    Task<Long> fib92 = create(92);
    TaskContext taskContext = MemoizingContext.composeWith(TaskContext.inmem());
    TaskContext.Value<Long> value = taskContext.evaluate(fib92);

    value.consume(f92 -> System.out.println("fib(92) = " + f92));
  }

  static Task<Long> create(long n) {
    TaskBuilder<Long> fib = Task.named("Fib", n).ofType(Long.class);
    if (n < 2) {
      return fib
          .process(() -> n);
    } else {
      return fib
          .in(() -> Fib.create(n - 1))
          .in(() -> Fib.create(n - 2))
          .process(Fib::fib);
    }
  }

  static long fib(long a, long b) {
    System.out.println("Fib.process(" + a + " + " + b + ") = " + (a + b));
    return a + b;
  }
}
