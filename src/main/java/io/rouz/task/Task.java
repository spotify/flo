package io.rouz.task;

import com.google.auto.value.AutoValue;

import io.rouz.task.dsl.TaskBuilder;
import io.rouz.task.dsl.TaskBuilder.F1;
import io.rouz.task.dsl.TaskBuilder.F2;
import io.rouz.task.dsl.TaskBuilder.F3;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;

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
public abstract class Task<T> {

  private static final Logger LOG = LoggerFactory.getLogger(Task.class);

  public abstract TaskId id();

  abstract Stream<? extends Task<?>> inputs();
  abstract Function<TaskContext, T> code();

  public Stream<TaskId> tasksInOrder() {
    return tasksInOrder(new LinkedHashSet<>());
  }

  // fixme: this can only be called once per task instance because it operates on the inputs stream
  protected Stream<TaskId> tasksInOrder(Set<TaskId> visits) {
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

      @Override
      public <A> TaskBuilder1<List<A>> ins(Stream<Task<A>> aTasks) {
        return new TaskBuilder1<List<A>>() {
          @Override
          public <R> Task<R> process(F1<List<A>, R> code) {
            return create(aTasks, code, taskName, args);
          }

          @Override
          public <B> TaskBuilder2<List<A>, B> in(Supplier<Task<B>> task) {
            return nope();
          }
        };
      }
    };
  }

  public static <T> Task<T> create(Supplier<T> code, String taskName, Object... args) {
    return create(toStream(), taskContext -> code.get(), taskName, args);
  }

  static <A,T> Task<T> create(
      Supplier<Task<A>> aTask,
      F1<A,T> code,
      String taskName,
      Object... args) {
    return create(
        toStream(aTask),
        taskContext -> code.apply(
            aTask.get().internalOut(taskContext)),
        taskName, args);
  }

  // fixme: consuming the stream here causes makes it only possible to get the output once
  // fixme: this is different from the supplier-based tasks
  static <A,T> Task<T> create(
      Stream<Task<A>> aTasks,
      F1<List<A>,T> code,
      String taskName,
      Object... args) {
    return Task.<T>create(
        aTasks,
        taskContext -> code.apply(
            aTasks.map(t -> t.internalOut(taskContext)).collect(toList())),
        taskName, args);
  }

  static <A,B,T> Task<T> create(
      Supplier<Task<A>> aTask,
      Supplier<Task<B>> bTask,
      F2<A,B,T> code,
      String taskName,
      Object... args) {
    return create(
        toStream(aTask, bTask),
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
        toStream(aTask, bTask, cTask),
        taskContext -> code.apply(
            aTask.get().internalOut(taskContext),
            bTask.get().internalOut(taskContext),
            cTask.get().internalOut(taskContext)),
        taskName, args);
  }

  static <T> Task<T> create(
      Stream<? extends Task<?>> inputs,
      Function<TaskContext, T> code,
      String taskName,
      Object... args) {
    final TaskId taskId = TaskIds.create(taskName, args);
    return new AutoValue_Task<>(taskId, inputs, memoize(taskId, code));
  }

  /**
   * Converts an array of {@link Supplier}s of {@link Task}s to a {@link Stream} of the same
   * {@link Task}s.
   *
   * It will only evaluate the {@link Task} instances (through calling {@link Supplier#get()})
   * when the returned {@link Stream} is consumed. Thus it retains lazyness.
   *
   * @param tasks  An array of tasks
   * @return A stream of the same tasks
   */
  @SafeVarargs
  private static Stream<Task<?>> toStream(Supplier<? extends Task<?>>... tasks) {
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

  private static <T> T nope() {
    throw new UnsupportedOperationException("not implemented");
  }
}
