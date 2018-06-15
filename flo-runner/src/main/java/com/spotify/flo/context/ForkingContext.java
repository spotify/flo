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
import com.spotify.flo.Fn;
import com.spotify.flo.TaskId;
import com.spotify.flo.Tracing;
import io.grpc.Context;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.Collections;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An {@link EvalContext} that runs tasks in sub-processes.
 * <p>
 * Forking can be disabled using the environment variable {@code FLO_DISABLE_FORKING=true}.
 * <p>
 * Forking is disabled by default when running in the debugger, but can be enabled by {@code FLO_FORCE_FORK=true}.
 * <p>
 * The basic idea here is to intercept the process fn by overriding {@link EvalContext#invokeProcessFn(TaskId, Fn)},
 * and executing it in a sub-process JVM. The process fn closure and the result is transported in to and out of the
 * sub-process using serialization.
 */
class ForkingContext extends ForwardingEvalContext {

  private static final Logger log = LoggerFactory.getLogger(ForkingContext.class);

  private ForkingContext(EvalContext delegate) {
    super(delegate);
  }

  static EvalContext composeWith(EvalContext baseContext) {
    // Is the Java Debug Wire Protocol activated?
    final boolean inDebugger = ManagementFactory.getRuntimeMXBean().
        getInputArguments().stream().anyMatch(s -> s.contains("-agentlib:jdwp"));

    final Optional<Boolean> forking = Optional.ofNullable(System.getenv("FLO_FORKING")).map(Boolean::parseBoolean);

    if (forking.isPresent()) {
      if (forking.get()) {
        log.debug("Forking enabled (environment variable FLO_FORKING=true)");
        return new ForkingContext(baseContext);
      } else {
        log.debug("Forking disabled (environment variable FLO_FORKING=true)");
        return baseContext;
      }
    } else if (inDebugger) {
      log.debug("Debugger detected, forking disabled by default "
          + "(enable by setting environment variable FLO_FORKING=true)");
      return baseContext;
    } else {
      log.debug("Debugger not detected, forking enabled by default "
          + "(disable by setting environment variable FLO_FORKING=false)");
      return new ForkingContext(baseContext);
    }
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
        return executor.execute(() -> {
          try {
            return Context.current()
                .withValue(Tracing.TASK_ID, taskId)
                .call(fn::get);
          } catch (RuntimeException e) {
            throw e;
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        });
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    };
  }
}
