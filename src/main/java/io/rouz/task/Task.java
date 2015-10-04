package io.rouz.task;

import com.google.auto.value.AutoValue;

import io.rouz.task.dsl.TaskBuilder;
import io.rouz.task.dsl.TaskBuilder.F1;
import io.rouz.task.dsl.TaskBuilder.F2;
import io.rouz.task.dsl.TaskBuilder.F3;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashSet;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

/**
 * TODO:
 * d make inputs lazily instantiated to allow short circuiting graph generation
 * . output task graph
 * . external outputs - outputs that might already be available (ie a file on disk)
 *   - can be implemented with a regular task, but better support can be added
 * . id task
 */
@AutoValue
public abstract class Task<T> {

  private static final Logger LOG = LoggerFactory.getLogger(Task.class);

  public abstract TaskId id();

  abstract Stream<Task<?>> inputs();
  abstract Function<TaskContext, T> code();

  public Stream<TaskId> tasksInOrder() {
    return tasksInOrder(new LinkedHashSet<>());
  }

  private Stream<TaskId> tasksInOrder(Set<TaskId> visits) {
    return inputs()
        .filter(task -> !visits.contains(task.id()))
        .flatMap(
            task -> {
              visits.add(task.id());
              return Stream.concat(
                  task.tasksInOrder(visits),
                  Stream.of(task.id()));
            });
  }

  public T out() {
    return code().apply(new TaskContextImpl());
  }

  private T internalOut(TaskContext taskContext) {
    return code().apply(taskContext);
  }

  public static TaskBuilder named(String taskName, Object... args) {
    return new TaskBuilder() {
      @Override
      public <R> Task<R> process(Supplier<R> code) {
        return create(code, taskName, args);
      }

      @Override
      public <A> TaskBuilder1<A> in(Supplier<Task<A>> aTask) {
        return new TaskBuilder1<A>() {
          @Override
          public <R> Task<R> process(F1<A, R> code) {
            return create(aTask, code, taskName, args);
          }

          @Override
          public <B> TaskBuilder2<A, B> in(Supplier<Task<B>> bTask) {
            return new TaskBuilder2<A, B>() {
              @Override
              public <R> Task<R> process(F2<A, B, R> code) {
                return create(aTask, bTask, code, taskName, args);
              }

              @Override
              public <C> TaskBuilder3<A, B, C> in(Supplier<Task<C>> cTask) {
                return new TaskBuilder3<A, B, C>() {
                  @Override
                  public <R> Task<R> process(F3<A, B, C, R> code) {
                    return create(aTask, bTask, cTask, code, taskName, args);
                  }
                };
              }
            };
          }
        };
      }
    };
  }

  public static <T> Task<T> create(Supplier<T> code, String taskName, Object... args) {
    return create(lazily(), taskContext -> code.get(), taskName, args);
  }

  static <A,T> Task<T> create(
      Supplier<Task<A>> aTask,
      F1<A,T> code,
      String taskName,
      Object... args) {
    return create(
        lazily(aTask),
        taskContext -> code.apply(
            aTask.get().internalOut(taskContext)),
        taskName, args);
  }

  static <A,B,T> Task<T> create(
      Supplier<Task<A>> aTask,
      Supplier<Task<B>> bTask,
      F2<A,B,T> code,
      String taskName,
      Object... args) {
    return create(
        lazily(aTask, bTask),
        taskContext -> code.apply(
            aTask.get().internalOut(taskContext),
            bTask.get().internalOut(taskContext)),
        taskName, args);
  }

  static <A,B,C,T> Task<T> create(
      Supplier<Task<A>> aTask,
      Supplier<Task<B>> bTask,
      Supplier<Task<C>> cTask,
      F3<A,B,C,T> code,
      String taskName,
      Object... args) {
    return create(
        lazily(aTask, bTask, cTask),
        taskContext -> code.apply(
            aTask.get().internalOut(taskContext),
            bTask.get().internalOut(taskContext),
            cTask.get().internalOut(taskContext)),
        taskName, args);
  }

  static <T> Task<T> create(
      Stream<Task<?>> inputs,
      Function<TaskContext, T> code,
      String taskName,
      Object... args) {
    final TaskId taskId = TaskIds.create(taskName, args);
    return new AutoValue_Task<>(taskId, inputs, memoize(taskId, code));
  }

  @SafeVarargs
  private static Stream<Task<?>> lazily(Supplier<? extends Task<?>>... tasks) {
    return Stream.of(tasks).map(Supplier::get);
  }

  private static <T> Function<TaskContext, T> memoize(TaskId taskId, Function<TaskContext, T> fn) {
    return taskContext -> {
      if (taskContext.has(taskId)) {
        final T value = taskContext.value(taskId);
        LOG.info("Found calculated value for {} = {}", taskId, value);
        return value;
      } else {
        final T value = fn.apply(taskContext);
        taskContext.put(taskId, value);
        return value;
      }
    };
  }
}
