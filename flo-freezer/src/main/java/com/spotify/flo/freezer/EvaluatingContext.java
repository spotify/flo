/*-
 * -\-\-
 * flo-freezer
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

package com.spotify.flo.freezer;

import static com.spotify.flo.freezer.PersistingContext.cleanForFilename;

import com.spotify.flo.EvalContext;
import com.spotify.flo.Fn;
import com.spotify.flo.Serialization;
import com.spotify.flo.Task;
import com.spotify.flo.TaskId;
import com.spotify.flo.context.ForwardingEvalContext;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A wrapper context that allows for evaluating specific tasks. This class is intended to be used
 * in pairing with {@link PersistingContext}.
 *
 * <p>See {@link #evaluateTaskFrom(Path)}.
 *
 * <p>todo: support reading and writing from arbitrary {@link Path} types
 */
public class EvaluatingContext {

  static final String OUTPUT_SUFFIX = "_out";

  private final Path basePath;
  private final EvalContext delegate;

  public EvaluatingContext(Path basePath, EvalContext delegate) {
    this.basePath = Objects.requireNonNull(basePath);
    this.delegate = Objects.requireNonNull(delegate);
  }

  /**
   * Evaluate a persisted task, expecting it's input values to exist as "_out" files in the same
   * directory.
   *
   * <p>The output of the evaluated task will be persisted in the same directory.
   *
   * @param persistedTask A path to the persisted task file that should be evaluated
   * @param <T>           The task output type
   * @return The task output value
   */
  public <T> EvalContext.Value<T> evaluateTaskFrom(Path persistedTask) {
    final Task<T> task;
    try {
      task = Serialization.deserialize(persistedTask);
    } catch (IOException | ClassNotFoundException e) {
      throw new RuntimeException(e);
    }

    return new SpecificEval(task, delegate).evaluate(task);
  }

  private Path resolveExistingOutput(TaskId taskId) {
    final String fileName = cleanForFilename(taskId) + OUTPUT_SUFFIX;
    return basePath.resolve(fileName);
  }

  private <T> void persist(TaskId taskId, T output) {
    final Path outputPath = basePath.resolve(cleanForFilename(taskId) + OUTPUT_SUFFIX);
    try {
      Serialization.serialize(output, outputPath);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private class SpecificEval extends ForwardingEvalContext {

    private final Task<?> evalTask;

    protected SpecificEval(Task<?> evalTask, EvalContext delegate) {
      super(delegate);
      this.evalTask = evalTask;
    }

    @Override
    public <T> Value<T> evaluateInternal(Task<T> task, EvalContext context) {
      final Promise<T> promise = promise();
      final TaskId id = task.id();
      final Set<TaskId> inputTaskIds = evalTask.inputs().stream()
          .map(Task::id)
          .collect(Collectors.toSet());

      if (inputTaskIds.contains(id)) {
        final Path inputValuePath = resolveExistingOutput(id);
        if (Files.exists(inputValuePath)) {
          final T value;
          try {
            value = Serialization.deserialize(inputValuePath);
            promise.set(value);
          } catch (IOException | ClassNotFoundException e) {
            promise.fail(e);
          }
        } else {
          promise.fail(new RuntimeException("Output value for input task " + id + " not found"));
        }
      } else if (!id.equals(evalTask.id())) {
        promise.fail(new RuntimeException("Evaluation of unexpected task: " + id));
      } else {
        final Value<T> tValue = super.evaluateInternal(task, context);
        tValue.consume(v -> persist(evalTask.id(), v));
        tValue.consume(promise::set);
        tValue.onFail(promise::fail);
      }

      return promise.value();
    }

    @Override
    public <T> Value<T> invokeProcessFn(TaskId taskId, Fn<T> processFn) {
      // todo: only invoke fn if taskId == evalTask
      // todo: fail if called for taskId != evalTask

      final Value<T> tValue = super.invokeProcessFn(taskId, processFn);
      tValue.consume(v -> LOG.info("{} == {}", taskId, v));
      return tValue;
    }
  }
}
