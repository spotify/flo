package io.rouz.task.context;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;

import io.rouz.task.Task;
import io.rouz.task.TaskContext;
import io.rouz.task.TaskId;
import io.rouz.task.dsl.TaskBuilder.F0;

/**
 * A {@link TaskContext} that evaluates tasks immediately and memoizes results in memory.
 *
 * Memoized results are tied to the instance the evaluated the values.
 */
public class InMemImmediateContext implements TaskContext {

  private Map<TaskId, Object> cache =  new HashMap<>();

  private InMemImmediateContext() {
  }

  public static TaskContext create() {
    return new InMemImmediateContext();
  }

  @Override
  public <T> Value<T> evaluate(Task<T> task) {
    final TaskId taskId =  task.id();

    final Value<T> value;
    if (has(taskId)) {
      value = get(taskId);
      LOG.debug("Found calculated value for {} = {}", taskId, value);
    } else {
      value = TaskContext.super.evaluate(task);
      put(taskId, value);
    }

    return value;
  }

  @Override
  public <T> Value<T> value(F0<T> value) {
    return new BlockingValue<>(value.get());
  }

  @Override
  public <T> Promise<T> promise() {
    return new ValuePromise<>();
  }

  private boolean has(TaskId taskId) {
    return cache.containsKey(taskId);
  }

  private <V> void put(TaskId taskId, V value) {
    cache.put(taskId, value);
  }

  private <V> V get(TaskId taskId) {
    //noinspection unchecked
    return (V) cache.get(taskId);
  }

  private final class BlockingValue<T> implements Value<T> {

    private final Semaphore setLatch;
    private final CountDownLatch releaseLatch;

    private volatile T value;
    private volatile Throwable error;

    private BlockingValue() {
      this.value = null;
      this.error = null;
      this.setLatch = new Semaphore(1);
      this.releaseLatch = new CountDownLatch(1);
    }

    private BlockingValue(T value) {
      this.value = Objects.requireNonNull(value);
      this.error = null;
      this.setLatch = new Semaphore(0);
      this.releaseLatch = new CountDownLatch(0);
    }

    @Override
    public TaskContext context() {
      return InMemImmediateContext.this;
    }

    @Override
    public <U> Value<U> flatMap(Function<? super T, ? extends Value<? extends U>> fn) {
      blockingWait();
      if (value != null) {
        //noinspection unchecked
        return (Value<U>) fn.apply(value);
      } else {
        //noinspection unchecked
        return (Value<U>) this;
      }
    }

    @Override
    public void consume(Consumer<T> consumer) {
      blockingWait();
      if (value != null) {
        consumer.accept(value);
      }
    }

    @Override
    public void onFail(Consumer<Throwable> errorConsumer) {
      blockingWait();
      if (error != null) {
        errorConsumer.accept(error);
      }
    }

    private void blockingWait() {
      try {
        releaseLatch.await();
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
  }

  private final class ValuePromise<T> implements Promise<T> {

    private final BlockingValue<T> value = new BlockingValue<>();

    @Override
    public Value<T> value() {
      return value;
    }

    @Override
    public void set(T t) {
      final boolean completed = value.setLatch.tryAcquire();
      if (!completed) {
        throw new IllegalStateException("Promise was already completed");
      } else {
        value.value = t;
        value.releaseLatch.countDown();
      }
    }

    @Override
    public void fail(Throwable throwable) {
      final boolean completed = value.setLatch.tryAcquire();
      if (!completed) {
        throw new IllegalStateException("Promise was already completed");
      } else {
        value.error = throwable;
        value.releaseLatch.countDown();
      }
    }
  }
}
