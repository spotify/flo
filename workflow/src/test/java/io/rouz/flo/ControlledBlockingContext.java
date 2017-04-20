package io.rouz.flo;

import static org.junit.Assert.fail;

import io.rouz.flo.TaskBuilder.F1;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * A {@link TaskContext} used for testing the concurrency behavior of task graphs.
 *
 * This context is not thread safe. The control methods (those not part of the {@link TaskContext}
 * interface) should not be used from multiple threads at the same time.
 */
class ControlledBlockingContext implements TaskContext {

  private static final int MAX_WAIT_MILLIS = 5000;

  private final Map<TaskId, Interceptor<?>> interceptors = new HashMap<>();
  private final Map<TaskId, CountDownLatch> awaiting = new HashMap<>();
  private final AtomicInteger activeCount = new AtomicInteger(0);

  /**
   * Blocks until {@code n} concurrent tasks are awaiting execution.
   *
   * @param nConcurrent  number of concurrent tasks to wait for
   * @throws InterruptedException if the blocked thread gets interrupted
   */
  void waitUntilNumConcurrent(int nConcurrent) throws InterruptedException {
    long t0 = System.currentTimeMillis();
    synchronized (activeCount) {
      while (activeCount.get() != nConcurrent) {
        if (System.currentTimeMillis() - t0 > MAX_WAIT_MILLIS) {
          fail("timeout while waiting for " + nConcurrent + " concurrent tasks to run");
        }
        activeCount.wait(MAX_WAIT_MILLIS);
      }
    }
  }

  /**
   * Blocks until the given task becomes available for execution.
   *
   * @param task  the task to wait for
   * @throws InterruptedException if the blocked thread gets interrupted
   */
  void waitFor(Task<?> task) throws InterruptedException {
    TaskId taskId = task.id();
    long t0 = System.currentTimeMillis();
    synchronized (activeCount) {
      while (!awaiting.containsKey(taskId)) {
        if (System.currentTimeMillis() - t0 > MAX_WAIT_MILLIS) {
          fail("timeout while waiting for task " + taskId + " to run");
        }
        activeCount.wait(MAX_WAIT_MILLIS);
      }
    }
  }

  /**
   * Releases a blocked task that is awaiting execution. Make sure to call {@link #waitFor(Task)}
   * before calling this as to ensure the task is really awaiting execution.
   *
   * @param task  the task to release
   */
  void release(Task<?> task) throws InterruptedException {
    awaiting.get(task.id()).countDown();
  }

  /**
   * Determines if a task is awaiting execution.
   *
   * @param task  the task queried about
   * @return true if the task is in the awaiting list
   */
  boolean isWaiting(Task<?> task) {
    return awaiting.keySet().contains(task.id());
  }

  /**
   * @return A set of awaiting tasks
   */
  Set<TaskId> waitingTasks() {
    return Collections.unmodifiableSet(awaiting.keySet());
  }

  /**
   * Intercept the creation of a task value.
   *
   * @param task  The task to intercept
   * @param fn    The interceptor
   * @param <T>   The value type of the task
   */
  <T> void intercept(Task<T> task, Interceptor<T> fn) {
    interceptors.put(task.id(), fn);
  }

  @Override
  public <T> Value<T> evaluate(Task<T> task) {
    TaskId taskId = task.id();
    CountDownLatch latch = new CountDownLatch(1);
    SettableValue<T> value = new SettableValue<>();

    synchronized (activeCount) {
      awaiting.put(taskId, latch);
      activeCount.incrementAndGet();
      activeCount.notifyAll();
    }

    Thread thread = new Thread(() -> {
      try {
        latch.await();
      } catch (InterruptedException e) {
        fail("interrupted");
      }

      TaskContext.super.evaluate(task).consume(v -> {
        value.setValue(v);
        synchronized (activeCount) {
          awaiting.remove(taskId);
          activeCount.decrementAndGet();
          activeCount.notifyAll();
        }
      });
    });
    thread.setDaemon(true);
    thread.start();

    return value;
  }

  @Override
  public <T> Value<T> invokeProcessFn(TaskId taskId, Fn<Value<T>> processFn) {
    //noinspection unchecked
    final Interceptor<T> interceptor = (Interceptor<T>) interceptors.get(taskId);
    if (interceptor != null) {
      LOG.info("Intercepting {}", taskId);
      return interceptor.apply(processFn);
    }

    return processFn.get();
  }

  @Override
  public <T> Value<T> value(Fn<T> value) {
    return new SettableValue<>(value.get());
  }

  @Override
  public <T> Promise<T> promise() {
    return new ValuePromise<>();
  }

  class SettableValue<T> implements Value<T> {

    private T value;
    private List<Consumer<T>> consumeQueue = new ArrayList<>();

    SettableValue() {
    }

    SettableValue(T value) {
      setValue(value);
    }

    @Override
    public TaskContext context() {
      return ControlledBlockingContext.this;
    }

    @Override
    public void consume(Consumer<T> consumer) {
      if (value == null) {
        consumeQueue.add(consumer);
      } else {
        consumer.accept(value);
      }
    }

    /**
     * Does nothing. Value can not fail, see {@link ValuePromise#fail(Throwable)}.
     */
    @Override
    public void onFail(Consumer<Throwable> errorConsumer) {
    }

    @Override
    public <U> Value<U> flatMap(Function<? super T, ? extends Value<? extends U>> fn) {
      SettableValue<U> uValue = new SettableValue<>();
      consume(t -> fn.apply(t).consume(uValue::setValue));
      return uValue;
    }

    void setValue(T value) {
      if (this.value != null) {
        throw new IllegalStateException(
            "Value set more than once. Was " + this.value + ", set " + value);
      }
      this.value = Objects.requireNonNull(value);

      List<Consumer<T>> consumers = consumeQueue;
      consumeQueue = null;

      for (Consumer<T> consumer : consumers) {
        consumer.accept(value);
      }
    }
  }

  class ValuePromise<T> implements Promise<T> {

    private final SettableValue<T> value = new SettableValue<>();

    @Override
    public Value<T> value() {
      return value;
    }

    @Override
    public void set(T t) {
      value.setValue(t);
    }

    @Override
    public void fail(Throwable throwable) {
      throw new UnsupportedOperationException();
    }
  }

  @FunctionalInterface
  interface Interceptor<T> extends F1<Fn<Value<T>>, Value<T>> {
  }
}
