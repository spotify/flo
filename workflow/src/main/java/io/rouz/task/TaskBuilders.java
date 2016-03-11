package io.rouz.task;

import java.io.Serializable;
import java.util.Collections;
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
import io.rouz.task.dsl.TaskBuilder.TaskBuilderC;
import io.rouz.task.dsl.TaskBuilder.TaskBuilderC0;
import io.rouz.task.dsl.TaskBuilder.TaskBuilderCV;
import io.rouz.task.dsl.TaskBuilder.TaskBuilderCV0;

import static java.util.stream.Collectors.toList;

/**
 * Package local implementation of the {@link TaskBuilder} tree.
 *
 * These classes tackle the exponential growth of paths that can be taken through the
 * {@link TaskBuilder}X interfaces by linearizing the implementation through composing functions.
 *
 * The linearization is implemented by letting the next builder in the chain take either a
 * {@link RecursiveEval} or {@link ChainingEval}. This evaluator allows the builder to chain
 * onto the evaluation by including more input tasks. The evaluator will finally be used to
 * terminate the builder by enclosing a function into an {@link EvalClosure} for a {@link Task}.
 */
final class TaskBuilders {

  static TaskBuilder rootBuilder(TaskId taskId) {
    return new Builder0(taskId);
  }

  // #############################################################################################

  private static class Builder0 extends BaseRefs implements TaskBuilder {

    Builder0(TaskId taskId) {
      super(taskId);
    }

    @Override
    public <R> Task<R> constant(F0<R> code) {
      return Task.create(inputs, tc -> tc.value(code), taskId);
    }

    @Override
    public <R> Task<R> processWithContext(F1<TaskContext, Value<R>> code) {
      return Task.create(inputs, code::apply, taskId);
    }

    @Override
    public <A> TaskBuilder1<A> in(F0<Task<A>> aTask) {
      F0<Task<A>> aTaskSingleton = Singleton.create(aTask);
      return new Builder1<>(
          lazyFlatten(inputs, lazyList(aTaskSingleton)),
          taskId,
          leafEvalFn(tc -> {
            Value<A> aValue = tc.evaluate(aTaskSingleton.get());
            return f1 -> aValue.map(f1::apply);
          }),
          leafEvalFn(tc -> {
            Value<A> aValue = tc.evaluate(aTaskSingleton.get());
            return f1 -> aValue.flatMap(f1::apply);
          }));
    }

    @Override
    public <A> TaskBuilder1<List<A>> ins(F0<List<Task<A>>> aTasks) {
      F0<List<Task<A>>> aTasksSingleton = Singleton.create(aTasks);
      return new Builder1<>(
          lazyFlatten(inputs, lazyFlatten(aTasksSingleton)),
          taskId,
          leafEvalFn(tc -> {
            Value<List<A>> aListValue = aTasksSingleton.get()
                .stream().map(tc::evaluate).collect(tc.toValueList());
            return f1 -> aListValue.map(f1::apply);
          }),
          leafEvalFn(tc -> {
            Value<List<A>> aListValue = aTasksSingleton.get()
                .stream().map(tc::evaluate).collect(tc.toValueList());
            return f1 -> aListValue.flatMap(f1::apply);
          }));
    }

    @Override
    public <Z> TaskBuilderC0<Z> curryTo() {
      return new BuilderC0<>(taskId);
    }

    @Override
    public <Z> TaskBuilderCV0<Z> curryToValue() {
      return new BuilderCV0<>(taskId);
    }
  }

  private static class BuilderC0<Z> extends BaseRefs implements TaskBuilderC0<Z> {

    BuilderC0(TaskId taskId) {
      super(taskId);
    }

    @Override
    public <A> TaskBuilderC<A, Z, Z> in(F0<Task<A>> aTask) {
      F0<Task<A>> aTaskSingleton = Singleton.create(aTask);
      return new BuilderC<>(
          lazyFlatten(inputs, lazyList(aTaskSingleton)),
          taskId,
          leafEval(
              tc -> tc.evaluate(aTaskSingleton.get())));
    }

    @Override
    public <A> TaskBuilderC<List<A>, Z, Z> ins(F0<List<Task<A>>> aTasks) {
      F0<List<Task<A>>> aTasksSingleton = Singleton.create(aTasks);
      return new BuilderC<>(
          lazyFlatten(inputs, lazyFlatten(aTasksSingleton)),
          taskId,
          leafEval(
              tc -> aTasksSingleton.get()
                  .stream().map(tc::evaluate).collect(tc.toValueList())));
    }
  }

  private static class BuilderCV0<Z> extends BaseRefs implements TaskBuilderCV0<Z> {

    BuilderCV0(TaskId taskId) {
      super(taskId);
    }

    @Override
    public <A> TaskBuilderCV<A, Value<Z>, Z> in(F0<Task<A>> aTask) {
      F0<Task<A>> aTaskSingleton = Singleton.create(aTask);
      return new BuilderCV<>(
          lazyFlatten(inputs, lazyList(aTaskSingleton)),
          taskId,
          leafValEval(
              tc -> tc.evaluate(aTaskSingleton.get())));
    }

    @Override
    public <A> TaskBuilderCV<List<A>, Value<Z>, Z> ins(F0<List<Task<A>>> aTasks) {
      F0<List<Task<A>>> aTasksSingleton = Singleton.create(aTasks);
      return new BuilderCV<>(
          lazyFlatten(inputs, lazyFlatten(aTasksSingleton)),
          taskId,
          leafValEval(
              tc -> aTasksSingleton.get()
                  .stream().map(tc::evaluate).collect(tc.toValueList())));
    }
  }

  // #############################################################################################

  private static class BuilderC<A, Y, Z> extends BaseRefs implements TaskBuilderC<A, Y, Z> {

    private final RecursiveEval<A, Y, Z> evaluator;

    private BuilderC(F0<List<Task<?>>> inputs, TaskId taskId, RecursiveEval<A, Y, Z> evaluator) {
      super(inputs, taskId);
      this.evaluator = evaluator;
    }

    @Override
    public Task<Z> process(F1<A, Y> fn) {
      return Task.create(inputs, evaluator.enclose(fn), taskId);
    }

    @Override
    public <B> TaskBuilderC<B, F1<A, Y>, Z> in(F0<Task<B>> bTask) {
      F0<Task<B>> bTaskSingleton = Singleton.create(bTask);
      return new BuilderC<>(
          lazyFlatten(inputs, lazyList(bTaskSingleton)),
          taskId,
          evaluator.curry(
              tc -> tc.evaluate(bTaskSingleton.get())));
    }

    @Override
    public <B> TaskBuilderC<List<B>, F1<A, Y>, Z> ins(F0<List<Task<B>>> bTasks) {
      F0<List<Task<B>>> bTasksSingleton = Singleton.create(bTasks);
      return new BuilderC<>(
          lazyFlatten(inputs, lazyFlatten(bTasksSingleton)),
          taskId,
          evaluator.curry(
              tc -> bTasksSingleton.get()
                  .stream().map(tc::evaluate).collect(tc.toValueList())));
    }
  }

  private static class BuilderCV<A, Y, Z> extends BaseRefs implements TaskBuilderCV<A, Y, Z> {

    private final RecursiveEval<A, Y, Z> evaluator;

    private BuilderCV(F0<List<Task<?>>> inputs, TaskId taskId, RecursiveEval<A, Y, Z> evaluator) {
      super(inputs, taskId);
      this.evaluator = evaluator;
    }

    @Override
    public Task<Z> process(F1<TaskContext, F1<A, Y>> code) {
      EvalClosure<Z> closure = tc -> evaluator.<Z>enclose((a) -> code.apply(tc).apply(a)).eval(tc);
      return Task.create(inputs, closure, taskId);
    }

    @Override
    public <B> TaskBuilderCV<B, F1<A, Y>, Z> in(F0<Task<B>> bTask) {
      F0<Task<B>> bTaskSingleton = Singleton.create(bTask);
      return new BuilderCV<>(
          lazyFlatten(inputs, lazyList(bTaskSingleton)),
          taskId,
          evaluator.curry(
              tc -> tc.evaluate(bTaskSingleton.get())));
    }

    @Override
    public <B> TaskBuilderCV<List<B>, F1<A, Y>, Z> ins(F0<List<Task<B>>> bTasks) {
      F0<List<Task<B>>> bTasksSingleton = Singleton.create(bTasks);
      return new BuilderCV<>(
          lazyFlatten(inputs, lazyFlatten(bTasksSingleton)),
          taskId,
          evaluator.curry(
              tc -> bTasksSingleton.get()
                  .stream().map(tc::evaluate).collect(tc.toValueList())));
    }
  }

  // #############################################################################################

  private static class Builder1<A> extends BaseRefs implements TaskBuilder1<A> {

    private final ChainingEval<F1<A, ?>> evaluator;
    private final ChainingEval<F1<A, Value<?>>> valEvaluator;

    Builder1(
        F0<List<Task<?>>> inputs,
        TaskId taskId,
        ChainingEval<F1<A, ?>> evaluator,
        ChainingEval<F1<A, Value<?>>> valEvaluator) {
      super(inputs, taskId);
      this.evaluator = evaluator;
      this.valEvaluator = valEvaluator;
    }

    @Override
    public <R> Task<R> process(F1<A, R> code) {
      return Task.create(inputs, evaluator.enclose(code), taskId);
    }

    @Override
    public <R> Task<R> processWithContext(F2<TaskContext, A, Value<R>> code) {
      EvalClosure<R> closure = tc -> valEvaluator.<R>enclose((a) -> code.apply(tc, a)).eval(tc);
      return Task.create(inputs, closure, taskId);
    }

    @Override
    public <B> TaskBuilder2<A, B> in(F0<Task<B>> bTask) {
      F0<Task<B>> bTaskSingleton = Singleton.create(bTask);
      return new Builder2<>(
          lazyFlatten(inputs, lazyList(bTaskSingleton)),
          taskId,
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
    public <B> TaskBuilder2<A, List<B>> ins(F0<List<Task<B>>> bTasks) {
      F0<List<Task<B>>> bTasksSingleton = Singleton.create(bTasks);
      return new Builder2<>(
          lazyFlatten(inputs, lazyFlatten(bTasksSingleton)),
          taskId,
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

  private static class Builder2<A, B> extends BaseRefs implements TaskBuilder2<A, B> {

    private final ChainingEval<F2<A, B, ?>> evaluator;
    private final ChainingEval<F2<A, B, Value<?>>> valEvaluator;

    Builder2(
        F0<List<Task<?>>> inputs,
        TaskId taskId,
        ChainingEval<F2<A, B, ?>> evaluator,
        ChainingEval<F2<A, B, Value<?>>> valEvaluator) {
      super(inputs, taskId);
      this.evaluator = evaluator;
      this.valEvaluator = valEvaluator;
    }

    @Override
    public <R> Task<R> process(F2<A, B, R> code) {
      return Task.create(inputs, evaluator.enclose(code), taskId);
    }

    @Override
    public <R> Task<R> processWithContext(F3<TaskContext, A, B, Value<R>> code) {
      EvalClosure<R> closure = tc -> valEvaluator.<R>enclose((a, b) -> code.apply(tc, a, b)).eval(tc);
      return Task.create(inputs, closure, taskId);
    }

    @Override
    public <C> TaskBuilder3<A, B, C> in(F0<Task<C>> cTask) {
      F0<Task<C>> cTaskSingleton = Singleton.create(cTask);
      return new Builder3<>(
          lazyFlatten(inputs, lazyList(cTaskSingleton)),
          taskId,
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
    public <C> TaskBuilder3<A, B, List<C>> ins(F0<List<Task<C>>> cTasks) {
      F0<List<Task<C>>> cTasksSingleton = Singleton.create(cTasks);
      return new Builder3<>(
          lazyFlatten(inputs, lazyFlatten(cTasksSingleton)),
          taskId,
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

  private static class Builder3<A, B, C> extends BaseRefs implements TaskBuilder3<A, B, C> {

    private final ChainingEval<F3<A, B, C, ?>> evaluator;
    private final ChainingEval<F3<A, B, C, Value<?>>> valEvaluator;

    Builder3(
        F0<List<Task<?>>> inputs,
        TaskId taskId,
        ChainingEval<F3<A, B, C, ?>> evaluator,
        ChainingEval<F3<A, B, C, Value<?>>> valEvaluator) {
      super(inputs, taskId);
      this.evaluator = evaluator;
      this.valEvaluator = valEvaluator;
    }

    @Override
    public <R> Task<R> process(F3<A, B, C, R> code) {
      return Task.create(inputs, evaluator.enclose(code), taskId);
    }

    @Override
    public <R> Task<R> processWithContext(F4<TaskContext, A, B, C, Value<R>> code) {
      EvalClosure<R> closure = tc -> valEvaluator.<R>enclose((a, b, c) -> code.apply(tc, a, b, c)).eval(tc);
      return Task.create(inputs, closure, taskId);
    }
  }

  // #############################################################################################

  /**
   * A convenience class for holding some reference. This is only so that we don't have to repeat
   * these declaration in every class above.
   */
  private static class BaseRefs {

    protected final F0<List<Task<?>>> inputs;
    protected final TaskId taskId;

    protected BaseRefs(TaskId taskId) {
      this(Collections::emptyList, taskId);
    }

    protected BaseRefs(F0<List<Task<?>>> inputs,TaskId taskId) {
      this.inputs = inputs;
      this.taskId = taskId;
    }
  }

  // #############################################################################################

  private static <A, B> RecursiveEval<A, B, B> leafEval(EvalClosure<A> aClosure) {
    return new RecursiveEval<>(aClosure, taskContext -> taskContext::immediateValue);
  }

  private static <A, B> RecursiveEval<A, Value<B>, B> leafValEval(EvalClosure<A> aClosure) {
    return new RecursiveEval<>(aClosure, taskContext -> val -> val);
  }

  private static <F> ChainingEval<F> leafEvalFn(F1<TaskContext, F1<F, Value<?>>> fClosure) {
    return new ChainingEval<>(fClosure);
  }

  private static final class RecursiveEval<A, B, Z> implements Serializable {

    private final EvalClosure<A> aClosure;
    private final F1<TaskContext, F1<B, Value<Z>>> contClosure;

    RecursiveEval(EvalClosure<A> aClosure, F1<TaskContext, F1<B, Value<Z>>> contClosure) {
      this.aClosure = aClosure;
      this.contClosure = contClosure;
    }

    public EvalClosure<Z> enclose(F1<A, B> fn) {
      return taskContext -> continuation(taskContext).apply(fn);
    }

    public <T> RecursiveEval<T, F1<A, B>, Z> curry(EvalClosure<T> tClosure) {
      return new RecursiveEval<>(tClosure, this::continuation);
    }

    private F1<F1<A, B>, Value<Z>> continuation(TaskContext taskContext) {
      F1<B, Value<Z>> cont = contClosure.apply(taskContext);
      Value<A> aVal = aClosure.eval(taskContext);

      return fn -> aVal.map(fn::apply).flatMap(cont::apply);
    }
  }

  private static final class ChainingEval<F> implements Serializable {

    private final F1<TaskContext, F1<F, Value<?>>> fClosure;

    ChainingEval(F1<TaskContext, F1<F, Value<?>>> fClosure) {
      this.fClosure = fClosure;
    }

    public <Z> EvalClosure<Z> enclose(F f) {
      //noinspection unchecked
      return taskContext -> (Value<Z>) fClosure.apply(taskContext).apply(f);
    }

    public <G> ChainingEval<G> chain(F1<TaskContext, F1<G, Value<F>>> mapClosure) {
      F1<TaskContext, F1<G, Value<?>>> continuation = tc -> {
        F1<G, Value<F>> fng = mapClosure.apply(tc);
        F1<F, Value<?>> fnf = fClosure.apply(tc);

        return g -> fng.apply(g).flatMap(fnf::apply);
      };
      return new ChainingEval<>(continuation);
    }
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
  private static F0<List<Task<?>>> lazyList(F0<? extends Task<?>>... tasks) {
    return () -> Stream.of(tasks)
        .map(F0::get)
        .collect(toList());
  }

  @SafeVarargs
  private static <T> F0<List<T>> lazyFlatten(F0<? extends List<? extends T>>... lists) {
    return () -> Stream.of(lists)
        .map(F0::get)
        .flatMap(List::stream)
        .collect(toList());
  }
}
