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

package com.spotify.flo;

import io.grpc.Context;
import java.util.function.Supplier;

public class FloTesting {

  private static final Context.Key<TestContext> CONTEXT = Context.key("test-context");

  public static TestScope scope() {
    return scopeWithContext(new TestContext());
  }

  public static TestScope scopeWithContext(TestContext testContext) {
    if (isTest()) {
      throw new IllegalStateException("Nested tests not supported");
    }
    return new TestScope(Context.current().withValue(CONTEXT, testContext));
  }

  public static void run(Runnable r) {
    try (TestScope ts = scope()) {
      r.run();
    }
  }

  public static <T> T supply(Supplier<T> f) {
    try (TestScope ts = scope()) {
      return f.get();
    }
  }

  public static boolean isTest() {
    return CONTEXT.get() != null;
  }

  public static TestContext context() {
    final TestContext context = CONTEXT.get();
    if (context == null) {
      throw new IllegalStateException("Not in test scope");
    }
    return context;
  }
}
