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
import com.spotify.flo.Fn;
import com.spotify.flo.Task;
import com.spotify.flo.TaskId;
import com.spotify.flo.TaskOperator;
import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;
import java.util.Map;
import java.util.Objects;

/**
 * A {@link EvalContext} that instruments the task expansion and invocation process.
 *
 * <p>This context will invoke methods on a {@link Listener} when tasks are evaluated through the
 * {@link #evaluate(Task)} method.
 *
 * <p>The {@link Listener#task(Task)} method is called for each discovered task in the evaluating
 * task tree. The inputs of a task are all also announced to this method before a task starts
 * processing.
 *
 * <p>The {@link Listener#status(TaskId, Listener.Phase)} method is called when a task is actually
 * being processed, i.e. when the {@link #invokeProcessFn(TaskId, Fn)} of that task in called in
 * the {@link EvalContext}. There will be at most two calls made for
 * each task. First with {@link Listener.Phase#START}, when evaluation starts. Then with either
 * {@link Listener.Phase#SUCCESS} or {@link Listener.Phase#FAILURE} depending on the success or
 * failure of the task {@link EvalContext.Value}.
 *
 */
public class InstrumentedContext extends ForwardingEvalContext {

  /**
   * A listener for instrumented evaluation. See {@link InstrumentedContext} for more details.
   */
  public interface Listener extends Closeable, Serializable {

    /**
     * Called when a {@link Task} is discovered.
     *
     * @param task The discovered task object
     */
    void task(Task<?> task);

    /**
     * Called when a task starts and finished it's evaluation. This will be called exactly twice
     * per evaluation of a task.
     *
     * @param task   The task that is being evaluated
     * @param phase  The phase of evaluation
     */
    void status(TaskId task, Phase phase);

    /**
     * Called to report some piece of task metadata.
     *
     * @param task   The task that is being evaluated
     * @param key  The metadata key.
     * @param value  The metadata value.
     */
    default void meta(TaskId task, String key, String value) {
    }

    /**
     * Called to report some piece of task metadata.
     *
     * By default this methods calls <code>meta(TaskId task, String key, String value)</code>
     * for each key-value pair.
     *
     * @param task The task that is being evaluated
     * @param data The key-value metadata
     */
    default void meta(TaskId task, Map<String, String> data) {
      data.forEach((key, value) -> meta(task, key, value));
    }

    /**
     * Called to close resources or connections used by the implementing class.
     *
     * @throws IOException
     */
    @Override
    default void close() throws IOException {}

    /**
     * The different phases of task evaluation.
     */
    enum Phase {
      /**
       * The task has started evaluating
       */
      START,

      /**
       * The task completed evaluating successfully
       */
      SUCCESS,

      /**
       * The task completed evaluating with a failure
       */
      FAILURE
    }
  }

  private final Listener listener;

  private InstrumentedContext(EvalContext baseContext, Listener listener) {
    super(baseContext);
    this.listener = Objects.requireNonNull(listener);
  }

  public static EvalContext composeWith(EvalContext baseContext, Listener listener) {
    return new InstrumentedContext(baseContext, listener);
  }

  @Override
  public <T> Value<T> evaluateInternal(Task<T> task, EvalContext context) {
    listener.task(task);
    return delegate.evaluateInternal(task, context);
  }

  @Override
  public <T> Value<T> invokeProcessFn(TaskId taskId, Fn<T> processFn) {
    return delegate.invokeProcessFn(taskId, () -> {
      listener.status(taskId, Listener.Phase.START);
      try {
        final T value = processFn.get();
        listener.status(taskId, Listener.Phase.SUCCESS);
        return value;
      } catch (Throwable t) {
        listener.status(taskId, Listener.Phase.FAILURE);
        throw t;
      }
    });
  }

  @Override
  public TaskOperator.Listener listener() {
    return super.listener().composeWith(new TaskOperator.Listener() {
      @Override
      public void meta(TaskId task, String key, String value) {
        listener.meta(task, key, value);
      }

      @Override
      public void meta(TaskId task, Map<String, String> data) {
        listener.meta(task, data);
      }
    });
  }
}
