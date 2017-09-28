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

import com.spotify.flo.context.AwaitingConsumer;
import org.hamcrest.FeatureMatcher;
import org.hamcrest.Matcher;

public class TestUtils {

  public static <T> T evalAndGet(Task<T> task) throws InterruptedException {
    AwaitingConsumer<T> val = new AwaitingConsumer<>();
    EvalContext.inmem().evaluate(task).consume(val);
    return val.awaitAndGet();
  }

  public static Throwable evalAndGetException(Task<?> task) throws InterruptedException {
    AwaitingConsumer<Throwable> val = new AwaitingConsumer<>();
    EvalContext.inmem().evaluate(task).onFail(val);
    return val.awaitAndGet();
  }

  public static <T> Matcher<Task<? extends T>> taskId(Matcher<TaskId> taskIdMatcher) {
    return new FeatureMatcher<Task<? extends T>, TaskId>(taskIdMatcher, "Task with id", "taskId") {
      @Override
      protected TaskId featureValueOf(Task<? extends T> actual) {
        return actual.id();
      }
    };
  }
}
