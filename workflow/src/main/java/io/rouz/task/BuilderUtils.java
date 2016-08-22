package io.rouz.task;

import java.io.Serializable;
import java.util.List;
import java.util.stream.Stream;

import io.rouz.task.dsl.TaskBuilder;

import static io.rouz.task.TaskContextWithId.withId;
import static java.util.stream.Collectors.toList;

/**
 * Internal utility functions for the {@link TaskBuilder} api implementation
 */
class BuilderUtils {

  private BuilderUtils() {
  }

  static <F> ChainingEval<F> leafEvalFn(TaskBuilder.F1<TaskContext, TaskBuilder.F1<F, TaskContext.Value<?>>> fClosure) {
    return new ChainingEval<>(fClosure);
  }

  static <A> TaskBuilder.F1<A, TaskContext.Value<?>> gated(
      TaskId taskId,
      TaskContext tc,
      TaskBuilder.F1<A, ?> f1) {
    return (a) -> tc.invokeProcessFn(taskId, () -> tc.immediateValue(f1.apply(a)));
  }

  static <A> TaskBuilder.F1<A, TaskContext.Value<?>> gatedVal(
      TaskId taskId,
      TaskContext tc,
      TaskBuilder.F1<A, TaskContext.Value<?>> f1) {
    return (a) -> tc.invokeProcessFn(taskId, () -> f1.apply(a));
  }

  static <R> EvalClosure<R> gated(TaskId taskId, TaskBuilder.F0<R> code) {
    return tc -> tc.invokeProcessFn(taskId, () -> tc.value(code));
  }

  static <R> EvalClosure<R> gatedVal(
      TaskId taskId,
      TaskBuilder.F1<TaskContext, TaskContext.Value<R>> code) {
    return tc -> tc.invokeProcessFn(taskId, () -> code.apply(withId(tc, taskId)));
  }

  /**
   * Converts an array of {@link TaskBuilder.F0}s of {@link Task}s to a {@link TaskBuilder.F0} of a list of
   * those tasks {@link Task}s.
   *
   * It will only evaluate the functions (through calling {@link TaskBuilder.F0#get()})
   * when the returned function is invoked. Thus it retains laziness.
   *
   * @param tasks  An array of lazy evaluated tasks
   * @return A function of a list of lazily evaluated tasks
   */
  @SafeVarargs
  static TaskBuilder.F0<List<Task<?>>> lazyList(TaskBuilder.F0<? extends Task<?>>... tasks) {
    return () -> Stream.of(tasks)
        .map(TaskBuilder.F0::get)
        .collect(toList());
  }

  @SafeVarargs
  static <T> TaskBuilder.F0<List<T>> lazyFlatten(TaskBuilder.F0<? extends List<? extends T>>... lists) {
    return () -> Stream.of(lists)
        .map(TaskBuilder.F0::get)
        .flatMap(List::stream)
        .collect(toList());
  }

  static final class ChainingEval<F> implements Serializable {

    private final TaskBuilder.F1<TaskContext, TaskBuilder.F1<F, TaskContext.Value<?>>> fClosure;

    ChainingEval(TaskBuilder.F1<TaskContext, TaskBuilder.F1<F, TaskContext.Value<?>>> fClosure) {
      this.fClosure = fClosure;
    }

    <Z> EvalClosure<Z> enclose(F f) {
      //noinspection unchecked
      return taskContext -> (TaskContext.Value<Z>) fClosure.apply(taskContext).apply(f);
    }

    <G> ChainingEval<G> chain(TaskBuilder.F1<TaskContext, TaskBuilder.F1<G, TaskContext.Value<F>>> mapClosure) {
      TaskBuilder.F1<TaskContext, TaskBuilder.F1<G, TaskContext.Value<?>>> continuation = tc -> {
        TaskBuilder.F1<G, TaskContext.Value<F>> fng = mapClosure.apply(tc);
        TaskBuilder.F1<F, TaskContext.Value<?>> fnf = fClosure.apply(tc);

        return g -> fng.apply(g).flatMap(fnf::apply);
      };
      return new ChainingEval<>(continuation);
    }
  }
}
