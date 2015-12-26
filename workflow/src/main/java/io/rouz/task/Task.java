package io.rouz.task;

import com.google.auto.value.AutoValue;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import io.rouz.task.dsl.TaskBuilder;
import io.rouz.task.dsl.TaskBuilder.F0;

/**
 * TODO:
 * d make inputs lazily instantiated to allow short circuiting graph generation
 * . output task graph
 * . external outputs - outputs that might already be available (ie a file on disk)
 *   - can be implemented with a regular task, but better support can be added
 * . id task
 *
 * {@code Task<T>(param1=value1, param2=valu2) => t}
 *
 * @param <T>  A type carrying the execution metadata of this task
 */
@AutoValue
public abstract class Task<T> implements Serializable {

  public abstract TaskId id();

  abstract EvalClosure<T> code();

  abstract F0<List<Task<?>>> lazyInputs();

  public List<Task<?>> inputs() {
    return lazyInputs().get();
  }

  public Stream<Task<?>> inputsInOrder() {
    return inputsInOrder(new HashSet<>());
  }

  private Stream<Task<?>> inputsInOrder(Set<TaskId> visits) {
    return inputs().stream()
        .filter(input -> !visits.contains(input.id()))
        .flatMap(input -> {
          visits.add(input.id());
          return Stream.concat(
              input.inputsInOrder(visits),
              Stream.of(input)
          );
        });
  }

  // FIXME: this method is too specific in it's TaskContext usage
  public T out() {
    final AtomicReference<T> ref = new AtomicReference<>();
    final CountDownLatch latch = new CountDownLatch(1);

    TaskContext.inmem()
        .evaluate(this)
        .consume(v -> {
          ref.set(v);
          latch.countDown();
        });

    try {
      latch.await(60, TimeUnit.SECONDS);
      return ref.get();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    throw new RuntimeException("waited for a minute, nothing happened");
  }

  public static TaskBuilder named(String taskName, Object... args) {
    return TaskBuilders.rootBuilder(taskName, args);
  }

  public static <T> Task<T> create(F0<T> code, String taskName, Object... args) {
    return create(Collections::emptyList, tc -> tc.value(code.get()), taskName, args);
  }

  static <T> Task<T> create(
      F0<List<Task<?>>> inputs,
      EvalClosure<T> code,
      String taskName,
      Object... args) {
    return new AutoValue_Task<>(TaskIds.create(taskName, args), code, inputs);
  }
}
