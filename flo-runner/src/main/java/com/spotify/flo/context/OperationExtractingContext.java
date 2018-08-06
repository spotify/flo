package com.spotify.flo.context;

import static com.spotify.flo.EvalContext.sync;

import com.spotify.flo.EvalContext;
import com.spotify.flo.Fn;
import com.spotify.flo.Operation;
import com.spotify.flo.Task;
import com.spotify.flo.TaskBuilder.F1;
import com.spotify.flo.TaskId;
import com.spotify.flo.TaskOperator;
import com.spotify.flo.TaskOperator.OperationException;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class OperationExtractingContext<T> implements EvalContext {

  private final Task<T> task;
  private final F1<TaskId, ?> inputValues;

  private OperationExtractingContext(Task<T> task, F1<TaskId, ?> inputValues) {
    this.task = Objects.requireNonNull(task, "task");
    this.inputValues = Objects.requireNonNull(inputValues, "inputValues");
    operator(task).orElseThrow(IllegalArgumentException::new);
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
    if (task.id().equals(this.task.id())) {
      return EvalContext.super.evaluateInternal(task, context);
    } else {
      final Object inputValue = inputValues.apply(task.id());
      if (inputValue == null) {
        throw new IllegalArgumentException("No value for input: " + task.id());
      }
      return (Value<U>) immediateValue(inputValue);
    }
  }

  public Operation extract() {
    final CompletableFuture<Throwable> f = new CompletableFuture<>();

    final Value<T> v = evaluate(task);

    v.consume(r -> f.completeExceptionally(new AssertionError()));
    v.onFail(f::complete);

    final Throwable t;
    try {
      t = f.get();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    } catch (ExecutionException e) {
      throw new RuntimeException(e);
    }

    if (!(t instanceof TaskOperator.OperationException)) {
      throw new AssertionError();
    }

    return ((OperationException) t).operation();
  }

  public static <T> OperationExtractingContext<T> of(Task<T> task, F1<TaskId, ?> inputValues) {
    return new OperationExtractingContext<>(task, inputValues);
  }

  public static <T> Operation extract(Task<T> task, F1<TaskId, ?> inputValues) {
    return of(task, inputValues).extract();
  }

  static Optional<? extends TaskOperator<?>> operator(Task<?> task) {
    return task.contexts().stream()
        .filter(ctx -> ctx instanceof TaskOperator)
        .map(ctx -> (TaskOperator<?>) ctx)
        .findAny();
  }

}
