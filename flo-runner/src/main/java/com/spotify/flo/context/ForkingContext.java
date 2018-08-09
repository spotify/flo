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
import com.spotify.flo.freezer.PersistingContext;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
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

  private final boolean dry;

  private ForkingContext(EvalContext delegate, boolean dry) {
    super(delegate);
    this.dry = dry;
  }

  static EvalContext composeWith(EvalContext baseContext) {
    return new ForkingContext(baseContext, false);
  }

  static EvalContext dryComposeWith(EvalContext baseContext) {
    return new ForkingContext(baseContext, true);
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
    if (dry) {
      LOG.debug("Dry run, forking disabled - testing serialization");
      return testFork(fn);
    } else if (FloTesting.isTest()) {
      LOG.debug("Test run, forking disabled - testing serialization");
      return testFork(fn);
    } else {
      return realFork(taskId, fn);
    }
  }

  private <T> Fn<T> testFork(Fn<T> fn) {
    // We do not currently have a mechanism for transporting mock inputs and outputs into and out of the task process.
    LOG.debug("Test run, forking disabled - testing serialization");
    return () -> {
      // Serialize & deserialize fn
      final ByteArrayOutputStream fnBaos = new ByteArrayOutputStream();
      PersistingContext.serialize(fn, fnBaos);
      final ByteArrayInputStream fnBais = new ByteArrayInputStream(fnBaos.toByteArray());
      final Fn<T> deserializedFn = PersistingContext.deserialize(fnBais);

      // Run the fn
      final T result = deserializedFn.get();

      // Serialize & deserialize the result
      final ByteArrayOutputStream resultBaos = new ByteArrayOutputStream();
      PersistingContext.serialize(result, resultBaos);
      final ByteArrayInputStream resultBais = new ByteArrayInputStream(resultBaos.toByteArray());
      final T deserializedResult = PersistingContext.deserialize(resultBais);

      return deserializedResult;
    };
  }

  private <T> Fn<T> realFork(TaskId taskId, Fn<T> fn) {
    return () -> {
      try (final ForkingExecutor executor = new ForkingExecutor()) {
        executor.environment(Collections.singletonMap("FLO_TASK_ID", taskId.toString()));
        return executor.execute(fn);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    };
  }
}
