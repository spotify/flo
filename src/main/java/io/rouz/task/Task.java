package io.rouz.task;

import com.google.auto.value.AutoValue;

import io.rouz.task.dsl.TaskBuilder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Function;
import java.util.function.Supplier;

/**
 * TODO:
 * d make inputs lazily instantiated to allow short circuiting graph generation
 * . output task graph
 * . external outputs - outputs that might already be available (ie a file on disk)
 *   - can be eimplemented with a regular task, but better support can be added
 * . id task
 */
@AutoValue
public abstract class Task<T> {

  private static final Logger LOG = LoggerFactory.getLogger(Task.class);

  public abstract TaskId id();

  abstract Function<TaskContext, T> code();

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
            return create(
                c -> code.apply(
                    aTask.get().internalOutput(c)),
                taskName, args);
          }

          @Override
          public <B> TaskBuilder2<A, B> in(Supplier<Task<B>> bTask) {
            return new TaskBuilder2<A, B>() {
              @Override
              public <R> Task<R> process(F2<A, B, R> code) {
                return create(
                    c -> code.apply(
                        aTask.get().internalOutput(c),
                        bTask.get().internalOutput(c)),
                    taskName, args);
              }

              @Override
              public <C> TaskBuilder3<A, B, C> in(Supplier<Task<C>> cTask) {
                return new TaskBuilder3<A, B, C>() {
                  @Override
                  public <R> Task<R> process(F3<A, B, C, R> code) {
                    return create(
                        c -> code.apply(
                            aTask.get().internalOutput(c),
                            bTask.get().internalOutput(c),
                            cTask.get().internalOutput(c)),
                        taskName, args);
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
    return create(taskContext -> code.get(), taskName, args);
  }

  static <T> Task<T> create(Function<TaskContext, T> code, String taskName, Object... args) {
    final TaskId taskId = TaskIds.create(taskName, args);
    return new AutoValue_Task<>(taskId, memoize(taskId, code));
  }

  public T output() {
    return code().apply(new TaskContextImpl());
  }

  private T internalOutput(TaskContext taskContext) {
    return code().apply(taskContext);
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
