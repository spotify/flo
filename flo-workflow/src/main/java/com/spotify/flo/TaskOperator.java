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

import static com.spotify.flo.BuilderUtils.guardedCall;

import java.io.Serializable;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;

/**
 * An operator controls the execution of a job for a task,  e.g. a data processing job on some processing platform.
 *
 * <p>The concrete operator implementation should {@link #provide(EvalContext)} the task with some means of constructing
 * an operation description. The operation description should be returned from the process fn.
 */
public interface TaskOperator<ContextT, SpecT, ResultT>
    extends TaskContext<ContextT, ResultT> {

  /**
   * Start an operation and wait for completion.
   */
  @SuppressWarnings("unchecked")
  default ResultT perform(SpecT spec, Listener listener) {
    final Operation<ResultT, ?> operation = start(spec, listener);
    Optional state = Optional.empty();
    Operation.Result<ResultT, ?> result;
    while (true) {
      result = operation.perform(state, listener);
      if (result.isDone()) {
        break;
      }
      state = result.state();
      try {
        Thread.sleep(result.pollInterval().toMillis());
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(e);
      }
    }
    if (result.isSuccess()) {
      return result.output();
    } else {
      final Throwable cause = result.cause();
      if (cause instanceof RuntimeException) {
        throw (RuntimeException) cause;
      } else if (cause instanceof Error) {
        throw (Error) cause;
      } else {
        throw new RuntimeException(cause);
      }
    }
  }

  /**
   * Start an asynchronous operation.
   * @return An {@link Operation} that can be used to check for completion.
   */
  default Operation<ResultT, ?> start(SpecT spec, Listener listener) {
    throw new UnsupportedOperationException();
  }

  /**
   * A callback for reporting task operation information.
   */
  @FunctionalInterface
  interface Listener extends Serializable {

    /**
     * Called to report some piece of task operation metadata.
     *
     * @param task The task that is being evaluated
     * @param data The key-value metadata
     */
    void meta(TaskId task, Map<String, String> data);

    Listener NOP = (task, data) -> { };

    default Listener composeWith(Listener listener) {
      return (task, data) -> {
        guardedCall(() -> Listener.this.meta(task, data));
        guardedCall(() -> listener.meta(task, data));
      };
    }
  }

  /**
   * An asynchronous operation.
   */
  interface Operation<ResultT, StateT> extends Serializable {

    /**
     * Attempt to make forward progress. Returns a {@link Result} indicating whether the operation is done and whether
     * it was successful or failed.
     */
    Result<ResultT, StateT> perform(Optional<StateT> state, Listener listener);

    /**
     * The result of an operation.
     */
    interface Result<ResultT, StateT> extends Serializable {

      /**
       * Returns true if this operation is done.
       */
      boolean isDone();

      /**
       * Returns true if this operation successfully completed.
       * @throws IllegalStateException if the operation is not yet done.
       */
      boolean isSuccess();

      /**
       * Returns the result if this operation successfully completed.
       * @throws IllegalStateException if the operation is not yet done or has failed.
       */
      ResultT output();

      /**
       * Returns the failure cause if the operation failed.
       * @throws IllegalStateException if the operation is not yet done or was successful.
       */
      Throwable cause();

      /**
       * Returns some state that should be passed back on the next call to {@link #perform(Optional, Listener)}.
       * @throws IllegalStateException if the operation is done.
       */
      Optional<StateT> state();

      /**
       * If the operation was not done, returns when {@link #perform(Optional, Listener)} should be called again.
       * @throws IllegalStateException if the operation is done.
       */
      Duration pollInterval();

      /**
       * Create a {@link Result} indicating that the operation is still ongoing.
       */
      static <ResultT> Result<ResultT, Void> ofContinuation(Duration pollInterval) {
        return ofContinuation(pollInterval, null);
      }

      /**
       * Create a {@link Result} indicating that the operation is still ongoing.
       */
      static <ResultT, StateT> Result<ResultT, StateT> ofContinuation(Duration pollInterval, StateT state) {
        return new Result<ResultT, StateT>() {
          @Override
          public boolean isDone() {
            return false;
          }

          @Override
          public ResultT output() {
            throw new IllegalStateException("not done");
          }

          @Override
          public Optional<StateT> state() {
            return Optional.ofNullable(state);
          }

          @Override
          public Duration pollInterval() {
            return pollInterval;
          }

          @Override
          public boolean isSuccess() {
            throw new IllegalStateException("not done");
          }

          @Override
          public Throwable cause() {
            throw new IllegalStateException("not done");
          }
        };
      }

      /**
       * Create a {@link Result} indicating that the operation successfully completed.
       */
      static <ResultT, StateT> Result<ResultT, StateT> ofSuccess(ResultT output) {
        return new Result<ResultT, StateT>() {
          @Override
          public boolean isDone() {
            return true;
          }

          @Override
          public ResultT output() {
            return output;
          }

          @Override
          public Optional<StateT> state() {
            return Optional.empty();
          }

          @Override
          public Duration pollInterval() {
            throw new IllegalStateException("operation is done");
          }

          @Override
          public boolean isSuccess() {
            return true;
          }

          @Override
          public Throwable cause() {
            throw new IllegalStateException("operation is a success");
          }
        };
      }

      /**
       * Create a {@link Result} indicating that the operation failed.
       */
      static <ResultT, StateT> Result<ResultT, StateT> ofFailure(Throwable cause) {
        return new Result<ResultT, StateT>() {
          @Override
          public boolean isDone() {
            return true;
          }

          @Override
          public ResultT output() {
            throw new IllegalStateException("operation is a failure");
          }

          @Override
          public Optional<StateT> state() {
            return Optional.empty();
          }

          @Override
          public Duration pollInterval() {
            throw new IllegalStateException("operation is done");
          }

          @Override
          public boolean isSuccess() {
            return false;
          }

          @Override
          public Throwable cause() {
            return cause;
          }
        };
      }
    }
  }
}
