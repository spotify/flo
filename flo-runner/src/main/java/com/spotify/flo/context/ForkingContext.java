/*-
 * -\-\-
 * Flo Runner
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
import com.spotify.flo.FloTesting;
import com.spotify.flo.Fn;
import com.spotify.flo.TaskId;
import com.spotify.flo.TestContext;
import com.spotify.flo.TestScope;
import com.spotify.flo.Tracing;
import io.grpc.Context;
import java.io.IOException;
import java.util.Collections;

/**
 * An {@link EvalContext} that runs tasks in sub-processes.
 * <p>
 * The basic idea here is to intercept the process fn by overriding {@link EvalContext#invokeProcessFn(TaskId, Fn)},
 * and executing it in a sub-process JVM. The process fn closure and the result is transported in to and out of the
 * sub-process using serialization.
 */
class ForkingContext extends ForwardingEvalContext {

  private ForkingContext(EvalContext delegate) {
    super(delegate);
  }

  static EvalContext composeWith(EvalContext baseContext) {
    return new ForkingContext(baseContext);
  }

  @Override
  public <T> Value<T> invokeProcessFn(TaskId taskId, Fn<T> processFn) {
    if (delegate == null) {
      throw new UnsupportedOperationException("nested execution not supported");
    }
    // Wrap the process fn in a lambda that will execute the original process fn closure in a sub-process.
    final Fn<T> forkingProcessFn = fork(taskId, processFn);
    // Pass on the wrapped process fn to let the rest of EvalContexts do their thing. The last EvalContext in the chain
    // will invoke the wrapped lambda, causing the sub-process execution to happen there.
    return delegate.invokeProcessFn(taskId, forkingProcessFn);
  }

  private <T> Fn<T> fork(TaskId taskId, Fn<T> fn) {
    return () -> {
      try (final ForkingExecutor executor = new ForkingExecutor()) {
        executor.environment(Collections.singletonMap("FLO_TASK_ID", taskId.toString()));
        final TestContext testInput = FloTesting.isTest() ? FloTesting.context().inputOnly() : null;
        final Result<T> result = executor.execute(() -> {
          try {
            return Context.current()
                .withValue(Tracing.TASK_ID, taskId)
                .call(() -> {
                  if (testInput != null) {
                    try (TestScope scope = FloTesting.scopeWithContext(testInput)) {
                      return Result.of(testInput, fn.get());
                    }
                  } else {
                    return Result.of(fn.get());
                  }
                });
          } catch (RuntimeException e) {
            throw e;
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        });
        if (FloTesting.isTest()) {
          // merge the output from inside the task process into the main test context;
          FloTesting.context().mergeOutput(result.testContext);
        }
        return result.value;
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    };
  }

  private static class Result<T> {

    final TestContext testContext;
    final T value;

    private Result(TestContext testContext, T value) {
      this.testContext = testContext;
      this.value = value;
    }

    private static <T> Result<T> of(TestContext context, T result) {
      return new Result<>(context, result);
    }

    private static <T> Result<T> of(T result) {
      return new Result<>(null, result);
    }
  }
}
