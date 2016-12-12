package io.rouz.flo;

import java.io.Serializable;
import java.util.List;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;

/**
 * Internal utility functions for the {@link TaskBuilder} api implementation
 */
class BuilderUtils {

  private BuilderUtils() {
  }

  static <F, Z> ChainingEval<F, Z> leafEvalFn(Fn1<TaskContext, Fn1<F, TaskContext.Value<Z>>> fClosure) {
    return new ChainingEval<>(fClosure);
  }

  static <A, Z> Fn1<A, TaskContext.Value<Z>> gated(
      TaskId taskId,
      TaskContext tc,
      Fn1<A, Z> f1) {
    return (a) -> tc.invokeProcessFn(taskId, () -> tc.immediateValue(f1.apply(a)));
  }

  static <A, Z> Fn1<A, TaskContext.Value<Z>> gatedVal(
      TaskId taskId,
      TaskContext tc,
      Fn1<A, TaskContext.Value<Z>> f1) {
    return (a) -> tc.invokeProcessFn(taskId, () -> f1.apply(a));
  }

  static <R> EvalClosure<R> gated(TaskId taskId, Fn<R> code) {
    return tc -> tc.invokeProcessFn(taskId, () -> tc.value(code));
  }

  static <R> EvalClosure<R> gatedVal(
      TaskId taskId,
      Fn1<TaskContext, TaskContext.Value<R>> code) {
    return tc -> tc.invokeProcessFn(taskId, () -> code.apply(TaskContextWithId.withId(tc, taskId)));
  }

  /**
   * Converts an array of {@link Fn}s of {@link Task}s to a {@link Fn} of a list of
   * those tasks {@link Task}s.
   *
   * It will only evaluate the functions (through calling {@link Fn#get()})
   * when the returned function is invoked. Thus it retains laziness.
   *
   * @param tasks  An array of lazy evaluated tasks
   * @return A function of a list of lazily evaluated tasks
   */
  @SafeVarargs
  static Fn<List<Task<?>>> lazyList(Fn<? extends Task<?>>... tasks) {
    return () -> Stream.of(tasks)
        .map(Fn::get)
        .collect(toList());
  }

  @SafeVarargs
  static <T> Fn<List<T>> lazyFlatten(Fn<? extends List<? extends T>>... lists) {
    return () -> Stream.of(lists)
        .map(Fn::get)
        .flatMap(List::stream)
        .collect(toList());
  }

  static final class ChainingEval<F, Z> implements Serializable {

    private final Fn1<TaskContext, Fn1<F, TaskContext.Value<Z>>> fClosure;

    ChainingEval(Fn1<TaskContext, Fn1<F, TaskContext.Value<Z>>> fClosure) {
      this.fClosure = fClosure;
    }

    EvalClosure<Z> enclose(F f) {
      return taskContext -> fClosure.apply(taskContext).apply(f);
    }

    <G> ChainingEval<G, Z> chain(Fn1<TaskContext, Fn1<G, F>> mapClosure) {
      Fn1<TaskContext, Fn1<G, TaskContext.Value<Z>>> continuation = tc -> {
        Fn1<G, F> fng = mapClosure.apply(tc);
        Fn1<F, TaskContext.Value<Z>> fnf = fClosure.apply(tc);

        return g -> fnf.apply(fng.apply(g));
      };
      return new ChainingEval<>(continuation);
    }
  }
}
