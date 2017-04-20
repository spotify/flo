package io.rouz.flo;

import com.google.auto.value.AutoValue;
import java.io.Serializable;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

/**
 * TODO:
 * d make inputs lazily instantiated to allow short circuiting graph generation
 * d output task graph
 * . external outputs - outputs that might already be available (ie a file on disk)
 *   - can be implemented with a regular task, but better support can be added
 * . identity task
 *
 * @param <T>  A type carrying the execution metadata of this task
 */
@AutoValue
public abstract class Task<T> implements Serializable {

  public abstract TaskId id();

  public abstract Class<T> type();

  abstract EvalClosure<T> code();

  abstract Fn<List<Task<?>>> lazyInputs();

  public List<Task<?>> inputs() {
    return lazyInputs().get();
  }

  public Stream<Task<?>> inputsInOrder() {
    return inputsInOrder(new HashSet<>());
  }

  private Stream<Task<?>> inputsInOrder(Set<TaskId> visits) {
    return inputs().stream()
        .filter(input -> !visits.contains(input.id()))
        .peek(input -> visits.add(input.id()))
        .flatMap(input -> Stream.concat(
            input.inputsInOrder(visits),
            Stream.of(input)
        ));
  }

  public static NamedTaskBuilder named(String taskName, Object... args) {
    return new NTB(TaskId.create(taskName, args));
  }

  public static <T> Task<T> create(Fn<T> code, Class<T> type, String taskName, Object... args) {
    return create(Collections::emptyList, type, tc -> tc.value(code), TaskId.create(taskName, args));
  }

  static <T> Task<T> create(
      Fn<List<Task<?>>> inputs, Class<T> type, EvalClosure<T> code, TaskId taskId) {
    return new AutoValue_Task<>(taskId, type, code, inputs);
  }

  /**
   * This is only needed because a generic lambda can not be implemented
   * in {@link #named(String, Object...)}.
   */
  private static final class NTB implements NamedTaskBuilder {

    private final TaskId taskId;

    private NTB(TaskId taskId) {
      this.taskId = taskId;
    }

    @Override
    public <Z> TaskBuilder<Z> ofType(Class<Z> type) {
      return TaskBuilderImpl.rootBuilder(taskId, type);
    }
  }
}
