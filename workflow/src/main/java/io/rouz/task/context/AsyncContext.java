package io.rouz.task.context;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;
import java.util.function.Function;

import io.rouz.task.Task;
import io.rouz.task.TaskContext;
import io.rouz.task.dsl.TaskBuilder.F0;

/**
 * A {@link TaskContext} that executes evaluation and {@link Value} computations on a given
 * {@link ExecutorService}.
 *
 * Override {@link #evaluate(Task)} to implement {@link Value} memoization.
 */
public class AsyncContext implements TaskContext {

  private final ExecutorService executor;

  protected AsyncContext(ExecutorService executor) {
    this.executor = Objects.requireNonNull(executor);
  }

  public static TaskContext create(ExecutorService executor) {
    return new AsyncContext(executor);
  }

  @Override
  public <T> Value<T> evaluate(Task<T> task) {
    return flatten(() -> TaskContext.super.evaluate(task));
  }

  @Override
  public final <T> Value<T> value(F0<T> t) {
    return new FutureValue<>(CompletableFuture.supplyAsync(t, executor));
  }

  @Override
  public final <T> Value<T> immediateValue(T t) {
    return new FutureValue<>(CompletableFuture.completedFuture(t));
  }

  protected final <T> Value<T> flatten(F0<Value<T>> t) {
    return flatten(CompletableFuture.supplyAsync(t, executor));
  }

  protected final <T> Value<T> flatten(CompletionStage<? extends Value<? extends T>> future) {
    final CompletableFuture<T> next = new CompletableFuture<>();
    future.whenCompleteAsync(
        (value, throwable) -> {
          if (throwable != null) {
            next.completeExceptionally(throwable);
          } else {
            value.consume(next::complete);
          }
        },
        executor);
    return new FutureValue<>(next);
  }

  private final class FutureValue<T> implements Value<T> {

    private final CompletionStage<T> future;

    private FutureValue(CompletionStage<T> future) {
      this.future = future;
    }

    @Override
    public TaskContext context() {
      return AsyncContext.this;
    }

    @Override
    public void consume(Consumer<T> consumer) {
      future.thenAccept(consumer);
    }

    @Override
    public <U> Value<U> flatMap(Function<? super T, ? extends Value<? extends U>> function) {
      return flatten(future.thenApply(function));
    }
  }
}
