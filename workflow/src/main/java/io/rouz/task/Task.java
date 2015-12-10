package io.rouz.task;

import com.google.auto.value.AutoValue;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import io.rouz.task.dsl.TaskBuilder;
import io.rouz.task.dsl.TaskBuilder.F0;
import io.rouz.task.dsl.TaskBuilder.F1;

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

  abstract F1<TaskContext, T> code();

  abstract F0<List<Task<?>>> lazyInputs();

  public List<Task<?>> inputs() {
    return lazyInputs().get();
  }

  public Stream<Task<?>> inputsInOrder() {
    return inputs().stream()
        .flatMap(input -> Stream.concat(
            input.inputsInOrder(),
            Stream.of(input)));
  }

  public T out() {
    return new TaskContextImpl().apply(id(), code());
  }

  T internalOut(TaskContext taskContext) {
    return taskContext.apply(id(), code());
  }

  public static TaskBuilder named(String taskName, Object... args) {
    return TaskBuilders.rootBuilder(taskName, args);
  }

  public static <T> Task<T> create(F0<T> code, String taskName, Object... args) {
    return create(Collections::emptyList, tc -> code.get(), taskName, args);
  }

  static <T> Task<T> create(
      F0<List<Task<?>>> inputs,
      F1<TaskContext, T> code,
      String taskName,
      Object... args) {
    return new AutoValue_Task<>(TaskIds.create(taskName, args), code, inputs);
  }
}
