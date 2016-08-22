package io.rouz.task;

import java.io.Serializable;
import java.util.List;
import java.util.stream.Stream;

import io.rouz.task.TaskContext.Value;
import io.rouz.task.dsl.TaskBuilder;
import io.rouz.task.dsl.TaskBuilder.F0;
import io.rouz.task.dsl.TaskBuilder.F1;
import io.rouz.task.dsl.TaskBuilder.F2;
import io.rouz.task.dsl.TaskBuilder.F3;
import io.rouz.task.dsl.TaskBuilder.F4;
import io.rouz.task.dsl.TaskBuilder.TaskBuilder1;
import io.rouz.task.dsl.TaskBuilder.TaskBuilder2;
import io.rouz.task.dsl.TaskBuilder.TaskBuilder3;

import static io.rouz.task.TaskContextWithId.withId;
import static java.util.stream.Collectors.toList;

/**
 * Package local implementation of the {@link TaskBuilder} tree.
 *
 * These classes tackle the exponential growth of paths that can be taken through the
 * {@link TaskBuilder}X interfaces by linearizing the implementation through composing functions.
 *
 * The linearization is implemented by letting the next builder in the chain take either a
 * {@link BuilderCurried.RecursiveEval} or {@link ChainingEval}. This evaluator allows the builder to chain
 * onto the evaluation by including more input tasks. The evaluator will finally be used to
 * terminate the builder by enclosing a function into an {@link EvalClosure} for a {@link Task}.
 */
final class TaskBuilders {

  static <Z> TaskBuilder<Z> rootBuilder(TaskId taskId, Class<Z> type) {
    return new Builder0<>(taskId, type);
  }

  // #############################################################################################

  private static class Builder0<Z> extends BaseRefs<Z> implements TaskBuilder<Z> {

    Builder0(TaskId taskId, Class<Z> type) {
      super(taskId, type);
    }

    @Override
    public Task<Z> process(F0<Z> code) {
      return Task.create(inputs, type, gated(taskId, code), taskId);
    }

    @Override
    public Task<Z> processWithContext(F1<TaskContext, Value<Z>> code) {
      return Task.create(inputs, type, gatedVal(taskId, code), taskId);
    }

    @Override
    public <A> TaskBuilder1<A, Z> in(F0<Task<A>> aTask) {
      F0<Task<A>> aTaskSingleton = Singleton.create(aTask);
      TaskId taskId = this.taskId; // local ref to drop ref to Builder0 instance
      return new Builder1<>(
          lazyFlatten(inputs, lazyList(aTaskSingleton)),
          taskId, type,
          leafEvalFn(tc -> {
            Value<A> aValue = tc.evaluate(aTaskSingleton.get());
            return f1 -> aValue.flatMap(gated(taskId, tc, f1));
          }),
          leafEvalFn(tc -> {
            Value<A> aValue = tc.evaluate(aTaskSingleton.get());
            return f1 -> aValue.flatMap(gatedVal(taskId, tc, f1));
          }));
    }

    @Override
    public <A> TaskBuilder1<List<A>, Z> ins(F0<List<Task<A>>> aTasks) {
      F0<List<Task<A>>> aTasksSingleton = Singleton.create(aTasks);
      TaskId taskId = this.taskId; // local ref to drop ref to Builder0 instance
      return new Builder1<>(
          lazyFlatten(inputs, lazyFlatten(aTasksSingleton)),
          taskId, type,
          leafEvalFn(tc -> {
            Value<List<A>> aListValue = aTasksSingleton.get()
                .stream().map(tc::evaluate).collect(tc.toValueList());
            return f1 -> aListValue.flatMap(gated(taskId, tc, f1));
          }),
          leafEvalFn(tc -> {
            Value<List<A>> aListValue = aTasksSingleton.get()
                .stream().map(tc::evaluate).collect(tc.toValueList());
            return f1 -> aListValue.flatMap(gatedVal(taskId, tc, f1));
          }));
    }

    @Override
    public TaskBuilderC0<Z> curried() {
      return new BuilderCurried.BuilderC0<>(taskId, type);
    }

    @Override
    public TaskBuilderCV0<Z> curriedWithContext() {
      return new BuilderCurried.BuilderCV0<>(taskId, type);
    }
  }

  // #############################################################################################

  private static class Builder1<A, Z> extends BaseRefs<Z> implements TaskBuilder1<A, Z> {

    private final ChainingEval<F1<A, ?>> evaluator;
    private final ChainingEval<F1<A, Value<?>>> valEvaluator;

    Builder1(
        F0<List<Task<?>>> inputs,
        TaskId taskId,
        Class<Z> type,
        ChainingEval<F1<A, ?>> evaluator,
        ChainingEval<F1<A, Value<?>>> valEvaluator) {
      super(inputs, taskId, type);
      this.evaluator = evaluator;
      this.valEvaluator = valEvaluator;
    }

    @Override
    public Task<Z> process(F1<A, Z> code) {
      return Task.create(inputs, type, evaluator.enclose(code), taskId);
    }

    @Override
    public Task<Z> processWithContext(F2<TaskContext, A, Value<Z>> code) {
      EvalClosure<Z> closure =
          tc -> valEvaluator.<Z>enclose((a) -> code.apply(withId(tc, taskId), a)).eval(tc);
      return Task.create(inputs, type, closure, taskId);
    }

    @Override
    public <B> TaskBuilder2<A, B, Z> in(F0<Task<B>> bTask) {
      F0<Task<B>> bTaskSingleton = Singleton.create(bTask);
      return new Builder2<>(
          lazyFlatten(inputs, lazyList(bTaskSingleton)),
          taskId, type,
          evaluator.chain(tc -> {
            Value<B> bValue = tc.evaluate(bTaskSingleton.get());
            return f2 -> bValue.map(b -> (a) -> f2.apply(a, b));
          }),
          valEvaluator.chain(tc -> {
            Value<B> bValue = tc.evaluate(bTaskSingleton.get());
            return f2 -> bValue.map(b -> (a) -> f2.apply(a, b));
          }));
    }

    @Override
    public <B> TaskBuilder2<A, List<B>, Z> ins(F0<List<Task<B>>> bTasks) {
      F0<List<Task<B>>> bTasksSingleton = Singleton.create(bTasks);
      return new Builder2<>(
          lazyFlatten(inputs, lazyFlatten(bTasksSingleton)),
          taskId, type,
          evaluator.chain(tc -> {
            Value<List<B>> bListValue = bTasksSingleton.get()
                .stream().map(tc::evaluate).collect(tc.toValueList());
            return f2 -> bListValue.map(b -> (a) -> f2.apply(a, b));
          }),
          valEvaluator.chain(tc -> {
            Value<List<B>> bListValue = bTasksSingleton.get()
                .stream().map(tc::evaluate).collect(tc.toValueList());
            return f2 -> bListValue.map(b -> (a) -> f2.apply(a, b));
          }));
    }
  }

  // #############################################################################################

  private static class Builder2<A, B, Z> extends BaseRefs<Z> implements TaskBuilder2<A, B, Z> {

    private final ChainingEval<F2<A, B, ?>> evaluator;
    private final ChainingEval<F2<A, B, Value<?>>> valEvaluator;

    Builder2(
        F0<List<Task<?>>> inputs,
        TaskId taskId,
        Class<Z> type,
        ChainingEval<F2<A, B, ?>> evaluator,
        ChainingEval<F2<A, B, Value<?>>> valEvaluator) {
      super(inputs, taskId, type);
      this.evaluator = evaluator;
      this.valEvaluator = valEvaluator;
    }

    @Override
    public Task<Z> process(F2<A, B, Z> code) {
      return Task.create(inputs, type, evaluator.enclose(code), taskId);
    }

    @Override
    public Task<Z> processWithContext(F3<TaskContext, A, B, Value<Z>> code) {
      EvalClosure<Z> closure =
          tc -> valEvaluator.<Z>enclose((a, b) -> code.apply(withId(tc, taskId), a, b)).eval(tc);
      return Task.create(inputs, type, closure, taskId);
    }

    @Override
    public <C> TaskBuilder3<A, B, C, Z> in(F0<Task<C>> cTask) {
      F0<Task<C>> cTaskSingleton = Singleton.create(cTask);
      return new Builder3<>(
          lazyFlatten(inputs, lazyList(cTaskSingleton)),
          taskId, type,
          evaluator.chain(tc -> {
            Value<C> cValue = tc.evaluate(cTaskSingleton.get());
            return f2 -> cValue.map(c -> (a, b) -> f2.apply(a, b, c));
          }),
          valEvaluator.chain(tc -> {
            Value<C> cValue = tc.evaluate(cTaskSingleton.get());
            return f2 -> cValue.map(c -> (a, b) -> f2.apply(a, b, c));
          }));
    }

    @Override
    public <C> TaskBuilder3<A, B, List<C>, Z> ins(F0<List<Task<C>>> cTasks) {
      F0<List<Task<C>>> cTasksSingleton = Singleton.create(cTasks);
      return new Builder3<>(
          lazyFlatten(inputs, lazyFlatten(cTasksSingleton)),
          taskId, type,
          evaluator.chain(tc -> {
            Value<List<C>> cListValue = cTasksSingleton.get()
                .stream().map(tc::evaluate).collect(tc.toValueList());
            return f3 -> cListValue.map(c -> (a, b) -> f3.apply(a, b, c));
          }),
          valEvaluator.chain(tc -> {
            Value<List<C>> cListValue = cTasksSingleton.get()
                .stream().map(tc::evaluate).collect(tc.toValueList());
            return f3 -> cListValue.map(c -> (a, b) -> f3.apply(a, b, c));
          }));
    }
  }

  // #############################################################################################

  private static class Builder3<A, B, C, Z> extends BaseRefs<Z> implements TaskBuilder3<A, B, C, Z> {

    private final ChainingEval<F3<A, B, C, ?>> evaluator;
    private final ChainingEval<F3<A, B, C, Value<?>>> valEvaluator;

    Builder3(
        F0<List<Task<?>>> inputs,
        TaskId taskId,
        Class<Z> type,
        ChainingEval<F3<A, B, C, ?>> evaluator,
        ChainingEval<F3<A, B, C, Value<?>>> valEvaluator) {
      super(inputs, taskId, type);
      this.evaluator = evaluator;
      this.valEvaluator = valEvaluator;
    }

    @Override
    public Task<Z> process(F3<A, B, C, Z> code) {
      return Task.create(inputs, type, evaluator.enclose(code), taskId);
    }

    @Override
    public Task<Z> processWithContext(F4<TaskContext, A, B, C, Value<Z>> code) {
      EvalClosure<Z> closure =
          tc -> valEvaluator.<Z>enclose((a, b, c) -> code.apply(withId(tc, taskId), a, b, c)).eval(tc);
      return Task.create(inputs, type, closure, taskId);
    }
  }

  // #############################################################################################

  private static <F> ChainingEval<F> leafEvalFn(F1<TaskContext, F1<F, Value<?>>> fClosure) {
    return new ChainingEval<>(fClosure);
  }

  private static final class ChainingEval<F> implements Serializable {

    private final F1<TaskContext, F1<F, Value<?>>> fClosure;

    ChainingEval(F1<TaskContext, F1<F, Value<?>>> fClosure) {
      this.fClosure = fClosure;
    }

    <Z> EvalClosure<Z> enclose(F f) {
      //noinspection unchecked
      return taskContext -> (Value<Z>) fClosure.apply(taskContext).apply(f);
    }

    <G> ChainingEval<G> chain(F1<TaskContext, F1<G, Value<F>>> mapClosure) {
      F1<TaskContext, F1<G, Value<?>>> continuation = tc -> {
        F1<G, Value<F>> fng = mapClosure.apply(tc);
        F1<F, Value<?>> fnf = fClosure.apply(tc);

        return g -> fng.apply(g).flatMap(fnf::apply);
      };
      return new ChainingEval<>(continuation);
    }
  }

  private static <A> F1<A, Value<?>> gated(TaskId taskId, TaskContext tc, F1<A, ?> f1) {
    return (a) -> tc.invokeProcessFn(taskId, () -> tc.immediateValue(f1.apply(a)));
  }

  private static <A> F1<A, Value<?>> gatedVal(TaskId taskId, TaskContext tc, F1<A, Value<?>> f1) {
    return (a) -> tc.invokeProcessFn(taskId, () -> f1.apply(a));
  }

  private static <R> EvalClosure<R> gated(TaskId taskId, F0<R> code) {
    return tc -> tc.invokeProcessFn(taskId, () -> tc.value(code));
  }

  private static <R> EvalClosure<R> gatedVal(TaskId taskId, F1<TaskContext, Value<R>> code) {
    return tc -> tc.invokeProcessFn(taskId, () -> code.apply(withId(tc, taskId)));
  }

  /**
   * Converts an array of {@link F0}s of {@link Task}s to a {@link F0} of a list of
   * those tasks {@link Task}s.
   *
   * It will only evaluate the functions (through calling {@link F0#get()})
   * when the returned function is invoked. Thus it retains laziness.
   *
   * @param tasks  An array of lazy evaluated tasks
   * @return A function of a list of lazily evaluated tasks
   */
  @SafeVarargs
  static F0<List<Task<?>>> lazyList(F0<? extends Task<?>>... tasks) {
    return () -> Stream.of(tasks)
        .map(F0::get)
        .collect(toList());
  }

  @SafeVarargs
  static <T> F0<List<T>> lazyFlatten(F0<? extends List<? extends T>>... lists) {
    return () -> Stream.of(lists)
        .map(F0::get)
        .flatMap(List::stream)
        .collect(toList());
  }
}
