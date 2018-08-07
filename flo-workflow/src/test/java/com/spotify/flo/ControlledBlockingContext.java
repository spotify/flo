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

import static org.junit.Assert.fail;

import com.spotify.flo.TaskBuilder.F1;
import com.spotify.flo.context.ForwardingEvalContext;
import com.spotify.flo.context.SyncContext;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A {@link EvalContext} used for testing the concurrency behavior of task graphs.
 *
 * This context is not thread safe. The control methods (those not part of the {@link EvalContext}
 * interface) should not be used from multiple threads at the same time.
 */
class ControlledBlockingContext extends ForwardingEvalContext {

  private static final int MAX_WAIT_MILLIS = 5000;

  private final Map<TaskId, Interceptor<?>> interceptors = new HashMap<>();
  private final Map<TaskId, CountDownLatch> awaiting = new HashMap<>();
  private final AtomicInteger activeCount = new AtomicInteger(0);

  ControlledBlockingContext() {
    super(SyncContext.create());
  }

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
  void release(Task<?> task) {
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
  public <T> Value<T> evaluateInternal(Task<T> task, EvalContext context) {
    TaskId taskId = task.id();
    CountDownLatch latch = new CountDownLatch(1);
    Promise<T> promise = promise();

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

      delegate.evaluateInternal(task, context).consume(v -> {
        promise.set(v);
        synchronized (activeCount) {
          awaiting.remove(taskId);
          activeCount.decrementAndGet();
          activeCount.notifyAll();
        }
      });
    });
    thread.setDaemon(true);
    thread.start();

    return promise.value();
  }

  @Override
  public <T> Value<T> invokeProcessFn(TaskId taskId, Fn<T> processFn) {
    //noinspection unchecked
    final Interceptor<T> interceptor = (Interceptor<T>) interceptors.get(taskId);
    if (interceptor != null) {
      LOG.info("Intercepting {}", taskId);
      return value(() -> interceptor.apply(processFn.get()));
    }

    return value(processFn);
  }

  @FunctionalInterface
  interface Interceptor<T> extends F1<T, T> {
  }
}
