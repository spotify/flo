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
import io.rouz.task.dsl.TaskBuilder.TaskBuilder1;
import io.rouz.task.dsl.TaskBuilder.TaskBuilder2;
import io.rouz.task.dsl.TaskBuilder.TaskBuilder3;
import io.rouz.task.dsl.TaskBuilder.TaskBuilderC;
import io.rouz.task.dsl.TaskBuilder.TaskBuilderC0;

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

  static TaskBuilder rootBuilder(String taskName, Object[] args) {
    return new Builder0(taskName, args);
  }

  // #############################################################################################

  private static class Builder0 extends BaseRefs implements TaskBuilder {

    Builder0(String taskName, Object[] args) {
      super(taskName, args);
    }

    @Override
    public <R> Task<R> constant(F0<R> code) {
      return Task.create(inputs, tc -> tc.value(code), taskName, args);
    }

    @Override
    public <A> TaskBuilder1<A> in(F0<Task<A>> aTask) {
      F0<Task<A>> aTaskSingleton = Singleton.create(aTask);
      return new Builder1<>(
          lazyFlatten(inputs, lazyList(aTaskSingleton)),
          taskName, args,
          leafEvalFn(tc -> {
            Value<A> aValue = tc.evaluate(aTaskSingleton.get());
            return f1 -> aValue.map(f1::apply);
          }));
    }

    @Override
    public <A> TaskBuilder1<List<A>> ins(F0<List<Task<A>>> aTasks) {
      F0<List<Task<A>>> aTasksSingleton = Singleton.create(aTasks);
      return new Builder1<>(
          lazyFlatten(inputs, lazyFlatten(aTasksSingleton)),
          taskName, args,
          leafEvalFn(tc -> {
            Value<List<A>> aListValue = aTasksSingleton.get()
                .stream().map(tc::evaluate).collect(tc.toValueList());
            return f1 -> aListValue.map(f1::apply);
          }));
    }

    @Override
    public <Z> TaskBuilderC0<Z> curryTo() {
      return new BuilderC0<>(taskName, args);
    }
  }

  private static class BuilderC0<Z> extends BaseRefs implements TaskBuilderC0<Z> {

    BuilderC0(String taskName, Object[] args) {
      super(taskName, args);
    }

    @Override
    public <A> TaskBuilderC<A, Z, Z> in(F0<Task<A>> aTask) {
      F0<Task<A>> aTaskSingleton = Singleton.create(aTask);
      return new BuilderC<>(
          lazyFlatten(inputs, lazyList(aTaskSingleton)),
          taskName, args,
          leafEval(
              tc -> tc.evaluate(aTaskSingleton.get())));
    }

    @Override
    public <A> TaskBuilderC<List<A>, Z, Z> ins(F0<List<Task<A>>> aTasks) {
      F0<List<Task<A>>> aTasksSingleton = Singleton.create(aTasks);
      return new BuilderC<>(
          lazyFlatten(inputs, lazyFlatten(aTasksSingleton)),
          taskName, args,
          leafEval(
              tc -> aTasksSingleton.get()
                  .stream().map(tc::evaluate).collect(tc.toValueList())));
    }
  }

  // #############################################################################################

  private static class BuilderC<A, Y, Z> extends BaseRefs implements TaskBuilderC<A, Y, Z> {

    private final RecursiveEval<A, Y, Z> evaluator;

    private BuilderC(
        F0<List<Task<?>>> inputs,
        String taskName,
        Object[] args,
        RecursiveEval<A, Y, Z> evaluator) {
      super(inputs, taskName, args);
      this.evaluator = evaluator;
    }

    @Override
    public Task<Z> process(F1<A, Y> fn) {
      return Task.create(inputs, evaluator.enclose(fn), taskName, args);
    }

    @Override
    public <B> TaskBuilderC<B, F1<A, Y>, Z> in(F0<Task<B>> bTask) {
      F0<Task<B>> bTaskSingleton = Singleton.create(bTask);
      return new BuilderC<>(
          lazyFlatten(inputs, lazyList(bTaskSingleton)),
          taskName, args,
          evaluator.curry(
              tc -> tc.evaluate(bTaskSingleton.get())));
    }

    @Override
    public <B> TaskBuilderC<List<B>, F1<A, Y>, Z> ins(F0<List<Task<B>>> bTasks) {
      F0<List<Task<B>>> bTasksSingleton = Singleton.create(bTasks);
      return new BuilderC<>(
          lazyFlatten(inputs, lazyFlatten(bTasksSingleton)),
          taskName, args,
          evaluator.curry(
              tc -> bTasksSingleton.get()
                  .stream().map(tc::evaluate).collect(tc.toValueList())));
    }
  }

  // #############################################################################################

  private static class Builder1<A> extends BaseRefs implements TaskBuilder1<A> {

    private final ChainingEval<F1<A, ?>> evaluator;

    Builder1(
        F0<List<Task<?>>> inputs,
        String taskName,
        Object[] args,
        ChainingEval<F1<A, ?>> evaluator) {
      super(inputs, taskName, args);
      this.evaluator = evaluator;
    }

    @Override
    public <R> Task<R> process(F1<A, R> code) {
      return Task.create(inputs, evaluator.enclose(code), taskName, args);
    }

    @Override
    public <B> TaskBuilder2<A, B> in(F0<Task<B>> bTask) {
      F0<Task<B>> bTaskSingleton = Singleton.create(bTask);
      return new Builder2<>(
          lazyFlatten(inputs, lazyList(bTaskSingleton)),
          taskName, args,
          evaluator.chain(tc -> {
            Value<B> bValue = tc.evaluate(bTaskSingleton.get());
            return f2 -> bValue.map(b -> (a) -> f2.apply(a, b));
          }));
    }

    @Override
    public <B> TaskBuilder2<A, List<B>> ins(F0<List<Task<B>>> bTasks) {
      F0<List<Task<B>>> bTasksSingleton = Singleton.create(bTasks);
      return new Builder2<>(
          lazyFlatten(inputs, lazyFlatten(bTasksSingleton)),
          taskName, args,
          evaluator.chain(tc -> {
            Value<List<B>> bListValue = bTasksSingleton.get()
                .stream().map(tc::evaluate).collect(tc.toValueList());
            return f2 -> bListValue.map(b -> (a) -> f2.apply(a, b));
          }));
    }
  }

  // #############################################################################################

  private static class Builder2<A, B> extends BaseRefs implements TaskBuilder2<A, B> {

    private final ChainingEval<F2<A, B, ?>> evaluator;

    Builder2(
        F0<List<Task<?>>> inputs,
        String taskName,
        Object[] args,
        ChainingEval<F2<A, B, ?>> evaluator) {
      super(inputs, taskName, args);
      this.evaluator = evaluator;
    }

    @Override
    public <R> Task<R> process(F2<A, B, R> code) {
      return Task.create(inputs, evaluator.enclose(code), taskName, args);
    }

    @Override
    public <C> TaskBuilder3<A, B, C> in(F0<Task<C>> cTask) {
      F0<Task<C>> cTaskSingleton = Singleton.create(cTask);
      return new Builder3<>(
          lazyFlatten(inputs, lazyList(cTaskSingleton)),
          taskName, args,
          evaluator.chain(tc -> {
            Value<C> cValue = tc.evaluate(cTaskSingleton.get());
            return f2 -> cValue.map(c -> (a, b) -> f2.apply(a, b, c));
          }));
    }

    @Override
    public <C> TaskBuilder3<A, B, List<C>> ins(F0<List<Task<C>>> cTasks) {
      F0<List<Task<C>>> cTasksSingleton = Singleton.create(cTasks);
      return new Builder3<>(
          lazyFlatten(inputs, lazyFlatten(cTasksSingleton)),
          taskName, args,
          evaluator.chain(tc -> {
            Value<List<C>> cListValue = cTasksSingleton.get()
                .stream().map(tc::evaluate).collect(tc.toValueList());
            return f3 -> cListValue.map(c -> (a, b) -> f3.apply(a, b, c));
          }));
    }
  }

  // #############################################################################################

  private static class Builder3<A, B, C> extends BaseRefs implements TaskBuilder3<A, B, C> {

    private final ChainingEval<F3<A, B, C, ?>> evaluator;

    Builder3(
        F0<List<Task<?>>> inputs,
        String taskName,
        Object[] args,
        ChainingEval<F3<A, B, C, ?>> evaluator) {
      super(inputs, taskName, args);
      this.evaluator = evaluator;
    }

    @Override
    public <R> Task<R> process(F3<A, B, C, R> code) {
      return Task.create(inputs, evaluator.enclose(code), taskName, args);
    }
  }

  // #############################################################################################

  /**
   * A convenience class for holding some reference. This is only so that we don't have to repeat
   * these declaration in every class above.
   */
  private static class BaseRefs {

    protected final F0<List<Task<?>>> inputs;
    protected final String taskName;
    protected final Object[] args;

    protected BaseRefs(String taskName, Object[] args) {
      this(Collections::emptyList, taskName, args);
    }

    protected BaseRefs(
        F0<List<Task<?>>> inputs,
        String taskName,
        Object[] args) {
      this.inputs = inputs;
      this.taskName = taskName;
      this.args = args;
    }
  }

  // #############################################################################################

  private static <A, B> RecursiveEval<A, B, B> leafEval(EvalClosure<A> aClosure) {
    return new RecursiveEval<>(aClosure, taskContext -> taskContext::immediateValue);
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
