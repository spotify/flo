package io.rouz.flo;

import java.io.Serializable;
import java.util.List;

import static io.rouz.flo.TaskContextWithId.withId;

/**
 * Implementation of the curried {@link TaskBuilder} api
 */
class BuilderCurried {

  private BuilderCurried() {
  }

  private static <A, B> RecursiveEval<A, B, B> leafEval(
      TaskId taskId,
      EvalClosure<A> aClosure) {
    return new RecursiveEval<>(true, taskId, aClosure, taskContext -> taskContext::immediateValue);
  }

  private static <A, B> RecursiveEval<A, TaskContext.Value<B>, B> leafValEval(
      TaskId taskId,
      EvalClosure<A> aClosure) {
    return new RecursiveEval<>(true, taskId, aClosure, taskContext -> val -> val);
  }

  static class BuilderC0<Z> extends BaseRefs<Z> implements CurriedTaskBuilder.TaskBuilderC0<Z> {

    BuilderC0(TaskId taskId, Class<Z> type) {
      super(taskId, type);
    }

    @Override
    public <A> CurriedTaskBuilder.TaskBuilderC<A, Z, Z> in(Fn<Task<A>> aTask) {
      Fn<Task<A>> aTaskSingleton = Singleton.create(aTask);
      return new BuilderC<>(
          BuilderUtils.lazyFlatten(inputs, BuilderUtils.lazyList(aTaskSingleton)),
          taskId, type,
          leafEval(
              taskId,
              tc -> tc.evaluate(aTaskSingleton.get())));
    }

    @Override
    public <A> CurriedTaskBuilder.TaskBuilderC<List<A>, Z, Z> ins(Fn<List<Task<A>>> aTasks) {
      Fn<List<Task<A>>> aTasksSingleton = Singleton.create(aTasks);
      return new BuilderC<>(
          BuilderUtils.lazyFlatten(inputs, BuilderUtils.lazyFlatten(aTasksSingleton)),
          taskId, type,
          leafEval(
              taskId,
              tc -> aTasksSingleton.get()
                  .stream().map(tc::evaluate).collect(tc.toValueList())));
    }
  }

  static class BuilderCV0<Z> extends BaseRefs<Z> implements CurriedTaskBuilder.TaskBuilderCV0<Z> {

    BuilderCV0(TaskId taskId, Class<Z> type) {
      super(taskId, type);
    }

    @Override
    public <A> CurriedTaskBuilder.TaskBuilderCV<A, TaskContext.Value<Z>, Z> in(Fn<Task<A>> aTask) {
      Fn<Task<A>> aTaskSingleton = Singleton.create(aTask);
      return new BuilderCV<>(
          BuilderUtils.lazyFlatten(inputs, BuilderUtils.lazyList(aTaskSingleton)),
          taskId, type,
          leafValEval(
              taskId,
              tc -> tc.evaluate(aTaskSingleton.get())));
    }

    @Override
    public <A> CurriedTaskBuilder.TaskBuilderCV<List<A>, TaskContext.Value<Z>, Z> ins(Fn<List<Task<A>>> aTasks) {
      Fn<List<Task<A>>> aTasksSingleton = Singleton.create(aTasks);
      return new BuilderCV<>(
          BuilderUtils.lazyFlatten(inputs, BuilderUtils.lazyFlatten(aTasksSingleton)),
          taskId, type,
          leafValEval(
              taskId,
              tc -> aTasksSingleton.get()
                  .stream().map(tc::evaluate).collect(tc.toValueList())));
    }
  }

  private static class BuilderC<A, Y, Z> extends BaseRefs<Z> implements CurriedTaskBuilder.TaskBuilderC<A, Y, Z> {

    private final RecursiveEval<A, Y, Z> evaluator;

    private BuilderC(Fn<List<Task<?>>> inputs, TaskId taskId, Class<Z> type, RecursiveEval<A, Y, Z> evaluator) {
      super(inputs, taskId, type);
      this.evaluator = evaluator;
    }

    @Override
    public Task<Z> process(Fn1<A, Y> fn) {
      return Task.create(inputs, type, evaluator.enclose(fn), taskId);
    }

    @Override
    public <B> CurriedTaskBuilder.TaskBuilderC<B, Fn1<A, Y>, Z> in(Fn<Task<B>> bTask) {
      Fn<Task<B>> bTaskSingleton = Singleton.create(bTask);
      return new BuilderC<>(
          BuilderUtils.lazyFlatten(inputs, BuilderUtils.lazyList(bTaskSingleton)),
          taskId, type,
          evaluator.curry(
              tc -> tc.evaluate(bTaskSingleton.get())));
    }

    @Override
    public <B> CurriedTaskBuilder.TaskBuilderC<List<B>, Fn1<A, Y>, Z> ins(Fn<List<Task<B>>> bTasks) {
      Fn<List<Task<B>>> bTasksSingleton = Singleton.create(bTasks);
      return new BuilderC<>(
          BuilderUtils.lazyFlatten(inputs, BuilderUtils.lazyFlatten(bTasksSingleton)),
          taskId, type,
          evaluator.curry(
              tc -> bTasksSingleton.get()
                  .stream().map(tc::evaluate).collect(tc.toValueList())));
    }
  }

  private static class BuilderCV<A, Y, Z> extends BaseRefs<Z> implements CurriedTaskBuilder.TaskBuilderCV<A, Y, Z> {

    private final RecursiveEval<A, Y, Z> evaluator;

    private BuilderCV(Fn<List<Task<?>>> inputs, TaskId taskId, Class<Z> type, RecursiveEval<A, Y, Z> evaluator) {
      super(inputs, taskId, type);
      this.evaluator = evaluator;
    }

    @Override
    public Task<Z> process(Fn1<TaskContext, Fn1<A, Y>> code) {
      EvalClosure<Z> closure =
          tc -> evaluator.<Z>enclose((a) -> code.apply(withId(tc, taskId)).apply(a)).eval(tc);
      return Task.create(inputs, type, closure, taskId);
    }

    @Override
    public <B> CurriedTaskBuilder.TaskBuilderCV<B, Fn1<A, Y>, Z> in(Fn<Task<B>> bTask) {
      Fn<Task<B>> bTaskSingleton = Singleton.create(bTask);
      return new BuilderCV<>(
          BuilderUtils.lazyFlatten(inputs, BuilderUtils.lazyList(bTaskSingleton)),
          taskId, type,
          evaluator.curry(
              tc -> tc.evaluate(bTaskSingleton.get())));
    }

    @Override
    public <B> CurriedTaskBuilder.TaskBuilderCV<List<B>, Fn1<A, Y>, Z> ins(Fn<List<Task<B>>> bTasks) {
      Fn<List<Task<B>>> bTasksSingleton = Singleton.create(bTasks);
      return new BuilderCV<>(
          BuilderUtils.lazyFlatten(inputs, BuilderUtils.lazyFlatten(bTasksSingleton)),
          taskId, type,
          evaluator.curry(
              tc -> bTasksSingleton.get()
                  .stream().map(tc::evaluate).collect(tc.toValueList())));
    }
  }

  private static final class RecursiveEval<A, B, Z> implements Serializable {

    private final boolean leaf;
    private final TaskId taskId;
    private final EvalClosure<A> aClosure;
    private final Fn1<TaskContext, Fn1<B, TaskContext.Value<Z>>> contClosure;

    RecursiveEval(
        boolean leaf,
        TaskId taskId,
        EvalClosure<A> aClosure,
        Fn1<TaskContext, Fn1<B, TaskContext.Value<Z>>> contClosure) {
      this.leaf = leaf;
      this.taskId = taskId;
      this.aClosure = aClosure;
      this.contClosure = contClosure;
    }

    EvalClosure<Z> enclose(Fn1<A, B> fn) {
      return taskContext -> continuation(taskContext).apply(fn);
    }

    <T> RecursiveEval<T, Fn1<A, B>, Z> curry(EvalClosure<T> tClosure) {
      return new RecursiveEval<>(false, taskId, tClosure, this::continuation);
    }

    private Fn1<Fn1<A, B>, TaskContext.Value<Z>> continuation(TaskContext taskContext) {
      Fn1<B, TaskContext.Value<Z>> cont = contClosure.apply(taskContext);
      TaskContext.Value<A> aVal = aClosure.eval(taskContext);

      return fn -> aVal.flatMap((a) -> (leaf)
          ? taskContext.invokeProcessFn(taskId, () -> cont.apply(fn.apply(a)))
          : cont.apply(fn.apply(a)));
    }
  }
}
