package io.rouz.task;

import java.io.Serializable;
import java.util.List;

import io.rouz.task.dsl.TaskBuilder;

import static io.rouz.task.BuilderUtils.lazyFlatten;
import static io.rouz.task.BuilderUtils.lazyList;
import static io.rouz.task.TaskContextWithId.withId;

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

  static class BuilderC0<Z> extends BaseRefs<Z> implements TaskBuilder.TaskBuilderC0<Z> {

    BuilderC0(TaskId taskId, Class<Z> type) {
      super(taskId, type);
    }

    @Override
    public <A> TaskBuilder.TaskBuilderC<A, Z, Z> in(TaskBuilder.F0<Task<A>> aTask) {
      TaskBuilder.F0<Task<A>> aTaskSingleton = Singleton.create(aTask);
      return new BuilderC<>(
          lazyFlatten(inputs, lazyList(aTaskSingleton)),
          taskId, type,
          leafEval(
              taskId,
              tc -> tc.evaluate(aTaskSingleton.get())));
    }

    @Override
    public <A> TaskBuilder.TaskBuilderC<List<A>, Z, Z> ins(TaskBuilder.F0<List<Task<A>>> aTasks) {
      TaskBuilder.F0<List<Task<A>>> aTasksSingleton = Singleton.create(aTasks);
      return new BuilderC<>(
          lazyFlatten(inputs, lazyFlatten(aTasksSingleton)),
          taskId, type,
          leafEval(
              taskId,
              tc -> aTasksSingleton.get()
                  .stream().map(tc::evaluate).collect(tc.toValueList())));
    }
  }

  static class BuilderCV0<Z> extends BaseRefs<Z> implements TaskBuilder.TaskBuilderCV0<Z> {

    BuilderCV0(TaskId taskId, Class<Z> type) {
      super(taskId, type);
    }

    @Override
    public <A> TaskBuilder.TaskBuilderCV<A, TaskContext.Value<Z>, Z> in(TaskBuilder.F0<Task<A>> aTask) {
      TaskBuilder.F0<Task<A>> aTaskSingleton = Singleton.create(aTask);
      return new BuilderCV<>(
          lazyFlatten(inputs, lazyList(aTaskSingleton)),
          taskId, type,
          leafValEval(
              taskId,
              tc -> tc.evaluate(aTaskSingleton.get())));
    }

    @Override
    public <A> TaskBuilder.TaskBuilderCV<List<A>, TaskContext.Value<Z>, Z> ins(TaskBuilder.F0<List<Task<A>>> aTasks) {
      TaskBuilder.F0<List<Task<A>>> aTasksSingleton = Singleton.create(aTasks);
      return new BuilderCV<>(
          lazyFlatten(inputs, lazyFlatten(aTasksSingleton)),
          taskId, type,
          leafValEval(
              taskId,
              tc -> aTasksSingleton.get()
                  .stream().map(tc::evaluate).collect(tc.toValueList())));
    }
  }

  private static class BuilderC<A, Y, Z> extends BaseRefs<Z> implements TaskBuilder.TaskBuilderC<A, Y, Z> {

    private final RecursiveEval<A, Y, Z> evaluator;

    private BuilderC(
        TaskBuilder.F0<List<Task<?>>> inputs, TaskId taskId, Class<Z> type, RecursiveEval<A, Y, Z> evaluator) {
      super(inputs, taskId, type);
      this.evaluator = evaluator;
    }

    @Override
    public Task<Z> process(TaskBuilder.F1<A, Y> fn) {
      return Task.create(inputs, type, evaluator.enclose(fn), taskId);
    }

    @Override
    public <B> TaskBuilder.TaskBuilderC<B, TaskBuilder.F1<A, Y>, Z> in(TaskBuilder.F0<Task<B>> bTask) {
      TaskBuilder.F0<Task<B>> bTaskSingleton = Singleton.create(bTask);
      return new BuilderC<>(
          lazyFlatten(inputs, lazyList(bTaskSingleton)),
          taskId, type,
          evaluator.curry(
              tc -> tc.evaluate(bTaskSingleton.get())));
    }

    @Override
    public <B> TaskBuilder.TaskBuilderC<List<B>, TaskBuilder.F1<A, Y>, Z> ins(TaskBuilder.F0<List<Task<B>>> bTasks) {
      TaskBuilder.F0<List<Task<B>>> bTasksSingleton = Singleton.create(bTasks);
      return new BuilderC<>(
          lazyFlatten(inputs, lazyFlatten(bTasksSingleton)),
          taskId, type,
          evaluator.curry(
              tc -> bTasksSingleton.get()
                  .stream().map(tc::evaluate).collect(tc.toValueList())));
    }
  }

  private static class BuilderCV<A, Y, Z> extends BaseRefs<Z> implements TaskBuilder.TaskBuilderCV<A, Y, Z> {

    private final RecursiveEval<A, Y, Z> evaluator;

    private BuilderCV(
        TaskBuilder.F0<List<Task<?>>> inputs, TaskId taskId, Class<Z> type, RecursiveEval<A, Y, Z> evaluator) {
      super(inputs, taskId, type);
      this.evaluator = evaluator;
    }

    @Override
    public Task<Z> process(TaskBuilder.F1<TaskContext, TaskBuilder.F1<A, Y>> code) {
      EvalClosure<Z> closure =
          tc -> evaluator.<Z>enclose((a) -> code.apply(withId(tc, taskId)).apply(a)).eval(tc);
      return Task.create(inputs, type, closure, taskId);
    }

    @Override
    public <B> TaskBuilder.TaskBuilderCV<B, TaskBuilder.F1<A, Y>, Z> in(TaskBuilder.F0<Task<B>> bTask) {
      TaskBuilder.F0<Task<B>> bTaskSingleton = Singleton.create(bTask);
      return new BuilderCV<>(
          lazyFlatten(inputs, lazyList(bTaskSingleton)),
          taskId, type,
          evaluator.curry(
              tc -> tc.evaluate(bTaskSingleton.get())));
    }

    @Override
    public <B> TaskBuilder.TaskBuilderCV<List<B>, TaskBuilder.F1<A, Y>, Z> ins(TaskBuilder.F0<List<Task<B>>> bTasks) {
      TaskBuilder.F0<List<Task<B>>> bTasksSingleton = Singleton.create(bTasks);
      return new BuilderCV<>(
          lazyFlatten(inputs, lazyFlatten(bTasksSingleton)),
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
    private final TaskBuilder.F1<TaskContext, TaskBuilder.F1<B, TaskContext.Value<Z>>> contClosure;

    RecursiveEval(
        boolean leaf,
        TaskId taskId,
        EvalClosure<A> aClosure,
        TaskBuilder.F1<TaskContext, TaskBuilder.F1<B, TaskContext.Value<Z>>> contClosure) {
      this.leaf = leaf;
      this.taskId = taskId;
      this.aClosure = aClosure;
      this.contClosure = contClosure;
    }

    EvalClosure<Z> enclose(TaskBuilder.F1<A, B> fn) {
      return taskContext -> continuation(taskContext).apply(fn);
    }

    <T> RecursiveEval<T, TaskBuilder.F1<A, B>, Z> curry(EvalClosure<T> tClosure) {
      return new RecursiveEval<>(false, taskId, tClosure, this::continuation);
    }

    private TaskBuilder.F1<TaskBuilder.F1<A, B>, TaskContext.Value<Z>> continuation(TaskContext taskContext) {
      TaskBuilder.F1<B, TaskContext.Value<Z>> cont = contClosure.apply(taskContext);
      TaskContext.Value<A> aVal = aClosure.eval(taskContext);

      return fn -> aVal.flatMap((a) -> (leaf)
          ? taskContext.invokeProcessFn(taskId, () -> cont.apply(fn.apply(a)))
          : cont.apply(fn.apply(a)));
    }
  }
}
