package io.rouz.task;

import com.google.auto.value.AutoValue;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * TODO:
 * d make inputs lazily instantiated to allow short circuiting graph generation
 * . output task graph
 * . external outputs - outputs that might already be available (ie a file on disk)
 *   - can be eimplemented with a regular task, but better support can be added
 */
@AutoValue
public abstract class Task<T> {

  public abstract TaskId id();

  abstract Function<TaskContext, T> code();

  public static TaskBuilder named(String taskName, Object... args) {
    return new TaskBuilder() {
      @Override
      public <A> TaskBuilder1<A> in(Supplier<Task<A>> aTask) {
        return new TaskBuilder1<A>() {
          @Override
          public <R> Task<R> process(F1<A, R> code) {
            return create(
                c -> code.apply(aTask.get().internalOutput(c)),
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
    final TaskContext taskContext = new TaskContextImpl();
    return code().apply(taskContext);
  }

  private T internalOutput(TaskContext taskContext) {
    return code().apply(taskContext);
  }

  private static <T> Function<TaskContext, T> memoize(TaskId taskId, Function<TaskContext, T> fn) {
    return taskContext -> {
      if (taskContext.has(taskId)) {
        final T value = taskContext.value(taskId);
        System.out.println("Found calculated value for " + taskId + " = " + value);
        return value;
      } else {
        final T value = fn.apply(taskContext);
        taskContext.put(taskId, value);
        return value;
      }
    };
  }

  public interface TaskId {
    String name();
    int hash();
  }

  @AutoValue
  static abstract class TaskIds implements TaskId {

    abstract List<Object> args();

    static TaskId create(String name, Object... args) {
      return new AutoValue_Task_TaskIds(name, Objects.hash(args), Arrays.asList(args));
    }

    @Override
    public String toString() {
      return name() + argsString() + String.format("#%08x", hash());
    }

    private String argsString() {
      final StringBuilder sb = new StringBuilder("(");
      boolean first = true;
      for (Object arg : args()) {
        if (!first) {
          sb.append(',');
        }
        sb.append(arg);
        first = false;
      }
      sb.append(')');

      return sb.toString();
    }
  }

  public interface TaskBuilder {
    <A> TaskBuilder1<A> in(Supplier<Task<A>> task);
  }

  public interface TaskBuilder1<A> {
    <R> Task<R> process(F1<A, R> code);
    <B> TaskBuilder2<A, B> in(Supplier<Task<B>> task);
  }

  public interface TaskBuilder2<A, B> {
    <R> Task<R> process(F2<A, B, R> code);
    <C> TaskBuilder3<A, B, C> in(Supplier<Task<C>> task);
  }

  public interface TaskBuilder3<A, B, C> {
    <R> Task<R> process(F3<A, B, C, R> code);
  }

  @FunctionalInterface
  public interface F1<A, R> {
    R apply(A a);
  }

  @FunctionalInterface
  public interface F2<A, B, R> {
    R apply(A a, B b);
  }

  @FunctionalInterface
  public interface F3<A, B, C, R> {
    R apply(A a, B b, C c);
  }

  interface TaskContext {
    boolean has(TaskId taskId);
    <V> V value(TaskId taskId);
    <V> void put(TaskId taskId, V value);
  }

  /**
   * Not thread safe, use with one thread only
   */
  private static class TaskContextImpl implements TaskContext {

    private Map<TaskId, Object> cache =  new HashMap<>();

    @Override
    public boolean has(TaskId taskId) {
      return cache.containsKey(taskId);
    }

    @Override
    public <V> V value(TaskId taskId) {
      //noinspection unchecked
      return (V) cache.get(taskId);
    }

    @Override
    public <V> void put(TaskId taskId, V value) {
      cache.put(taskId, value);
    }
  }
}
