package io.rouz.task;

import com.google.auto.value.AutoValue;

import io.rouz.task.dsl.TaskBuilder;

import java.io.ObjectStreamException;
import java.io.Serializable;
import java.util.List;
import java.util.stream.Stream;

/**
 * A mirror of the {@link Task} class that instead of a {@link Stream} of inputs has a
 * {@link List} of inputs. This is used as a serialization proxy for tasks via the
 * {@link Task#writeReplace()} and {@link #readResolve()} methods.
 */
@AutoValue
abstract class NonStreamTask<T> implements Serializable {

  abstract TaskId id();
  abstract List<Task<?>> inputs();
  abstract TaskBuilder.F1<TaskContext, T> code();

  protected Object readResolve() throws ObjectStreamException {
    return new AutoValue_Task<>(id(), inputs().stream(), code());
  }

  static <T> NonStreamTask<T> create(
      TaskId id,
      List<Task<?>> inputs,
      TaskBuilder.F1<TaskContext, T> code) {
    return new AutoValue_NonStreamTask<>(id, inputs, code);
  }
}
