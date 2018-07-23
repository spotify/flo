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
import com.spotify.flo.Task;
import com.spotify.flo.TaskOperator;
import com.spotify.flo.TaskOperator.SpecException;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An {@link EvalContext} that invokes a {@link TaskOperator} to compute a task result.
 */
public class OperatingContext extends ForwardingEvalContext {

  private static final Logger LOG = LoggerFactory.getLogger(OperatingContext.class);

  private final Logging logging;

  private OperatingContext(EvalContext delegate, Logging logging) {
    super(delegate);
    this.logging = logging;
  }

  public static EvalContext composeWith(EvalContext baseContext, Logging logging) {
    return new OperatingContext(baseContext, logging);
  }

  @Override
  public <T> Value<T> evaluateInternal(Task<T> task, EvalContext context) {
    final Optional<TaskOperator<?>> taskOperator =
        task.contexts().stream()
            .filter(c -> c instanceof TaskOperator)
            .<TaskOperator<?>>map(c -> (TaskOperator<?>) c)
            .findFirst();

    if (!taskOperator.isPresent()) {
      return delegate.evaluateInternal(task, context);
    }

    final TaskOperator operator = taskOperator.get();

    // Get job spec from process fn
    final Promise<T> promise = promise();
    final Value<T> spec = delegate.evaluateInternal(task, context);

    // TODO: Awful hack, can we come up with something better?
    // We get the job spec in a specially crafted exception to work around the difficult of returning something
    // from the process fn using a normal return.
    spec.onFail(e -> {
      if (e instanceof TaskOperator.SpecException) {
        @SuppressWarnings("unchecked") final Value<T> result =
            value(() -> (T) operator.run(task, ((SpecException) e).spec()));
        result.onFail(promise::fail);
        result.consume(promise::set);
      } else {
        promise.fail(e);
      }
    });

    // If we didn't get an exception, maybe the user returned the desired result directly.
    spec.consume(promise::set);

    return promise.value();
  }
}
