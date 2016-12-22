package io.rouz.flo;

import java.io.Serializable;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

import io.rouz.flo.TaskContext.Value;

import static io.rouz.flo.TaskContextWithId.withId;
import static java.util.stream.Collectors.toList;

/**
 * Internal utility functions for the {@link TaskBuilder} api implementation
 */
class BuilderUtils {

  private BuilderUtils() {
  }

  static <F, Z> ChainingEval<F, Z> leafEvalFn(EvalClosure<Fn1<F, Value<Z>>> fClosure) {
    return new ChainingEval<>(fClosure);
  }

  static <R> EvalClosure<R> gated(TaskId taskId, Fn<R> code) {
    return tc -> tc.invokeProcessFn(taskId, () -> tc.value(code));
  }

  static <R> EvalClosure<R> gatedVal(
      TaskId taskId,
      Fn1<TaskContext, Value<R>> code) {
    return tc -> tc.invokeProcessFn(taskId, () -> code.apply(withId(tc, taskId)));
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

    private final EvalClosure<Fn1<F, Value<Z>>> fClosure;

    ChainingEval(EvalClosure<Fn1<F, Value<Z>>> fClosure) {
      this.fClosure = fClosure;
    }

    EvalClosure<Z> enclose(F f) {
      return taskContext -> fClosure.eval(taskContext).flatMap(ff -> ff.apply(f));
    }

    <G> ChainingEval<G, Z> chain(EvalClosure<Fn1<G, F>> mapClosure) {
      EvalClosure<Fn1<G, Value<Z>>> continuation = tc -> {
        Value<Fn1<G, F>> gv = mapClosure.eval(tc);
        Value<Fn1<F, Value<Z>>> fv = fClosure.eval(tc);

        return tc.mapBoth(gv, fv, (gc, fc) -> g -> fc.apply(gc.apply(g)));
      };
      return new ChainingEval<>(continuation);
    }
  }
}
