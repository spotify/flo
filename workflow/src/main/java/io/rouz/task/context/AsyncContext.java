package io.rouz.task.context;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
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

  @Override
  public <T> Promise<T> promise() {
    return new FuturePromise<>();
  }

  protected final <T> Value<T> flatten(F0<Value<T>> t) {
    return flatten(CompletableFuture.supplyAsync(t, executor));
  }

  protected final <T> Value<T> flatten(CompletionStage<? extends Value<? extends T>> future) {
    final CompletableFuture<T> next = new CompletableFuture<>();
    future.whenCompleteAsync(
        (value, throwable) -> {
          if (throwable != null) {
            next.completeExceptionally(resolveAppThrowable(throwable));
          } else {
            value.consume(next::complete);
            value.onFail(next::completeExceptionally);
          }
        },
        executor);
    return new FutureValue<>(next);
  }

  private static Throwable resolveAppThrowable(Throwable throwable) {
    return (throwable instanceof CompletionException)
        ? resolveAppThrowable(throwable.getCause())
        : throwable;
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
      future.thenAcceptAsync(consumer, executor);
    }

    @Override
    public void onFail(Consumer<Throwable> errorConsumer) {
      future.whenCompleteAsync((ˍ, throwable) -> {
        if (throwable != null) {
          errorConsumer.accept(resolveAppThrowable(throwable));
        }
      }, executor);
    }

    @Override
    public <U> Value<U> map(Function<? super T, ? extends U> fn) {
      return new FutureValue<>(future.thenApplyAsync(fn, executor));
    }

    @Override
    public <U> Value<U> flatMap(Function<? super T, ? extends Value<? extends U>> function) {
      return flatten(future.thenApplyAsync(function, executor));
    }
  }

  private final class FuturePromise<T> implements Promise<T> {

    private final CompletableFuture<T> future = new CompletableFuture<>();
    private final FutureValue<T> value = new FutureValue<>(future);

    @Override
    public Value<T> value() {
      return value;
    }

    @Override
    public void set(T t) {
      final boolean completed = future.complete(t);
      if (!completed) {
        throw new IllegalStateException("Promise was already completed");
      }
    }

    @Override
    public void fail(Throwable throwable) {
      final boolean completed = future.completeExceptionally(throwable);
      if (!completed) {
        throw new IllegalStateException("Promise was already completed");
      }
    }
  }
}
