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
import com.spotify.flo.TaskOutput;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An {@link EvalContext} that may return a value without calling the processFn or evaluating its
 * dependencies.
 */
public class OverridingContext extends ForwardingEvalContext {
  private static final Logger LOG = LoggerFactory.getLogger(OverridingContext.class);
  private final Logging logging;

  private OverridingContext(EvalContext delegate, Logging logging) {
    super(delegate);
    this.logging = logging;
  }

  public static EvalContext composeWith(EvalContext baseContext, Logging logging) {
    return new OverridingContext(baseContext, logging);
  }

  @Override
  public <T> Value<T> evaluateInternal(Task<T> task, EvalContext context) {
    final Optional<TaskOutput<?, T>> taskOutput =
        task.contexts().stream()
            .filter(c -> c instanceof TaskOutput)
            .<TaskOutput<?, T>>map(c -> (TaskOutput<?, T>) c)
            .findFirst();

    if (taskOutput.isPresent()) {
      return context.value(() -> taskOutput.get().lookup(task))
          .flatMap(value -> {
            if (value.isPresent()) {
              final T t = value.get();
              LOG.debug("Not expanding {}, lookup = {}", task.id(), t);
              logging.overriddenValue(task.id(), t);
              return context.immediateValue(t);
            } else {
              LOG.debug("Lookup not found, expanding {}", task.id());
              logging.overriddenValueNotFound(task.id());
              return delegate.evaluateInternal(task, context);
            }
          });
    } else {
      LOG.debug("Expanding {}", task.id());
      return delegate.evaluateInternal(task, context);
    }
  }
}
