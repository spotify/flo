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

package com.spotify.flo;

import static com.spotify.flo.EvalContext.sync;
import static com.spotify.flo.EvalContextWithTask.withTask;

import com.spotify.flo.TaskBuilder.F1;
import java.io.Serializable;
import java.util.Objects;
import java.util.Optional;

public class OperationExtractingContext<T> implements EvalContext {

  private final Task<T> task;
  private final F1<TaskId, ?> inputValues;
  private final TaskOperator<?, ?, T> operator;

  private OperationExtractingContext(Task<T> task, F1<TaskId, ?> inputValues) {
    this.task = Objects.requireNonNull(task, "task");
    this.inputValues = Objects.requireNonNull(inputValues, "inputValues");
    this.operator = operator(task).orElseThrow(IllegalArgumentException::new);
  }

  @Override
  public <U> Value<U> value(Fn<U> value) {
    return sync().value(value);
  }

  @Override
  public <U> Promise<U> promise() {
    return sync().promise();
  }

  @SuppressWarnings("unchecked")
  @Override
  public <U> Value<U> evaluateInternal(Task<U> task, EvalContext context) {
    final Object inputValue = inputValues.apply(task.id());
    if (inputValue == null) {
      throw new IllegalArgumentException("No value for input: " + task.id());
    }
    return (Value<U>) immediateValue(inputValue);
  }

  public Operation<T> extract() {
    final EvalContext ec = withTask(this, task);
    final Object[] args = task.args().stream()
        .map(arg -> arg.get(ec))
        .map(value -> value.map(v -> (Object) v))
        .collect(Values.toValueList(ec))
        .get().toArray();

    final Object spec = task.processFn().invoke(args);

    return new Operation<>(operator, spec);
  }

  public static <T> OperationExtractingContext<T> of(Task<T> task, F1<TaskId, ?> inputValues) {
    return new OperationExtractingContext<>(task, inputValues);
  }

  public static <T> Operation extract(Task<T> task, F1<TaskId, ?> inputValues) {
    return of(task, inputValues).extract();
  }

  @SuppressWarnings("unchecked")
  public static <T> Optional<? extends TaskOperator<?, ?, T>> operator(Task<T> task) {
    return task.contexts().stream()
        .filter(ctx -> ctx instanceof TaskOperator)
        .map(ctx -> (TaskOperator<?, ?, T>) ctx)
        .findAny();
  }

  public static class Operation<T> implements Serializable {

    public static final long serialVersionUID = 1;

    public final TaskOperator<?, ?, T> operator;
    public final Object spec;

    public Operation(TaskOperator<?, ?, T> operator, Object spec) {
      this.operator = operator;
      this.spec = spec;
    }
  }

}
