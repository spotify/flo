package io.rouz.task;

import java.util.List;

import io.rouz.task.TaskContext.Value;

import static io.rouz.task.TaskContextWithId.withId;

/**
 * Package local implementation of the {@link TaskBuilder} tree.
 *
 * These classes tackle the exponential growth of paths that can be taken through the
 * {@link TaskBuilder}X interfaces by linearizing the implementation through composing functions.
 *
 * The linearization is implemented by letting the next builder in the chain take a
 * {@link BuilderUtils.ChainingEval}. This evaluator allows the builder to chain onto the evaluation by
 * including more input tasks. The evaluator will finally be used to terminate the builder by
 * enclosing a function into an {@link EvalClosure} for a {@link Task}.
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
      return Task.create(inputs, type, BuilderUtils.gated(taskId, code), taskId);
    }

    @Override
    public Task<Z> processWithContext(F1<TaskContext, Value<Z>> code) {
      return Task.create(inputs, type, BuilderUtils.gatedVal(taskId, code), taskId);
    }

    @Override
    public <A> TaskBuilder1<A, Z> in(F0<Task<A>> aTask) {
      F0<Task<A>> aTaskSingleton = Singleton.create(aTask);
      TaskId taskId = this.taskId; // local ref to drop ref to Builder0 instance
      return new Builder1<>(
          BuilderUtils.lazyFlatten(inputs, BuilderUtils.lazyList(aTaskSingleton)),
          taskId, type,
          BuilderUtils.leafEvalFn(tc -> {
            Value<A> aValue = tc.evaluate(aTaskSingleton.get());
            return f1 -> aValue.flatMap(BuilderUtils.gated(taskId, tc, f1));
          }),
          BuilderUtils.leafEvalFn(tc -> {
            Value<A> aValue = tc.evaluate(aTaskSingleton.get());
            return f1 -> aValue.flatMap(BuilderUtils.gatedVal(taskId, tc, f1));
          }));
    }

    @Override
    public <A> TaskBuilder1<List<A>, Z> ins(F0<List<Task<A>>> aTasks) {
      F0<List<Task<A>>> aTasksSingleton = Singleton.create(aTasks);
      TaskId taskId = this.taskId; // local ref to drop ref to Builder0 instance
      return new Builder1<>(
          BuilderUtils.lazyFlatten(inputs, BuilderUtils.lazyFlatten(aTasksSingleton)),
          taskId, type,
          BuilderUtils.leafEvalFn(tc -> {
            Value<List<A>> aListValue = aTasksSingleton.get()
                .stream().map(tc::evaluate).collect(tc.toValueList());
            return f1 -> aListValue.flatMap(BuilderUtils.gated(taskId, tc, f1));
          }),
          BuilderUtils.leafEvalFn(tc -> {
            Value<List<A>> aListValue = aTasksSingleton.get()
                .stream().map(tc::evaluate).collect(tc.toValueList());
            return f1 -> aListValue.flatMap(BuilderUtils.gatedVal(taskId, tc, f1));
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

  private static class Builder1<A, Z> extends BaseRefs<Z> implements TaskBuilder.TaskBuilder1<A, Z> {

    private final BuilderUtils.ChainingEval<TaskBuilder.F1<A, ?>> evaluator;
    private final BuilderUtils.ChainingEval<TaskBuilder.F1<A, Value<?>>> valEvaluator;

    Builder1(
        TaskBuilder.F0<List<Task<?>>> inputs,
        TaskId taskId,
        Class<Z> type,
        BuilderUtils.ChainingEval<TaskBuilder.F1<A, ?>> evaluator,
        BuilderUtils.ChainingEval<TaskBuilder.F1<A, Value<?>>> valEvaluator) {
      super(inputs, taskId, type);
      this.evaluator = evaluator;
      this.valEvaluator = valEvaluator;
    }

    @Override
    public Task<Z> process(TaskBuilder.F1<A, Z> code) {
      return Task.create(inputs, type, evaluator.enclose(code), taskId);
    }

    @Override
    public Task<Z> processWithContext(TaskBuilder.F2<TaskContext, A, Value<Z>> code) {
      EvalClosure<Z> closure =
          tc -> valEvaluator.<Z>enclose((a) -> code.apply(withId(tc, taskId), a)).eval(tc);
      return Task.create(inputs, type, closure, taskId);
    }

    @Override
    public <B> TaskBuilder.TaskBuilder2<A, B, Z> in(TaskBuilder.F0<Task<B>> bTask) {
      TaskBuilder.F0<Task<B>> bTaskSingleton = Singleton.create(bTask);
      return new Builder2<>(
          BuilderUtils.lazyFlatten(inputs, BuilderUtils.lazyList(bTaskSingleton)),
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
    public <B> TaskBuilder.TaskBuilder2<A, List<B>, Z> ins(TaskBuilder.F0<List<Task<B>>> bTasks) {
      TaskBuilder.F0<List<Task<B>>> bTasksSingleton = Singleton.create(bTasks);
      return new Builder2<>(
          BuilderUtils.lazyFlatten(inputs, BuilderUtils.lazyFlatten(bTasksSingleton)),
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

  private static class Builder2<A, B, Z> extends BaseRefs<Z> implements TaskBuilder.TaskBuilder2<A, B, Z> {

    private final BuilderUtils.ChainingEval<TaskBuilder.F2<A, B, ?>> evaluator;
    private final BuilderUtils.ChainingEval<TaskBuilder.F2<A, B, Value<?>>> valEvaluator;

    Builder2(
        TaskBuilder.F0<List<Task<?>>> inputs,
        TaskId taskId,
        Class<Z> type,
        BuilderUtils.ChainingEval<TaskBuilder.F2<A, B, ?>> evaluator,
        BuilderUtils.ChainingEval<TaskBuilder.F2<A, B, Value<?>>> valEvaluator) {
      super(inputs, taskId, type);
      this.evaluator = evaluator;
      this.valEvaluator = valEvaluator;
    }

    @Override
    public Task<Z> process(TaskBuilder.F2<A, B, Z> code) {
      return Task.create(inputs, type, evaluator.enclose(code), taskId);
    }

    @Override
    public Task<Z> processWithContext(TaskBuilder.F3<TaskContext, A, B, Value<Z>> code) {
      EvalClosure<Z> closure =
          tc -> valEvaluator.<Z>enclose((a, b) -> code.apply(withId(tc, taskId), a, b)).eval(tc);
      return Task.create(inputs, type, closure, taskId);
    }

    @Override
    public <C> TaskBuilder.TaskBuilder3<A, B, C, Z> in(TaskBuilder.F0<Task<C>> cTask) {
      TaskBuilder.F0<Task<C>> cTaskSingleton = Singleton.create(cTask);
      return new Builder3<>(
          BuilderUtils.lazyFlatten(inputs, BuilderUtils.lazyList(cTaskSingleton)),
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
    public <C> TaskBuilder.TaskBuilder3<A, B, List<C>, Z> ins(TaskBuilder.F0<List<Task<C>>> cTasks) {
      TaskBuilder.F0<List<Task<C>>> cTasksSingleton = Singleton.create(cTasks);
      return new Builder3<>(
          BuilderUtils.lazyFlatten(inputs, BuilderUtils.lazyFlatten(cTasksSingleton)),
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

  private static class Builder3<A, B, C, Z> extends BaseRefs<Z> implements TaskBuilder.TaskBuilder3<A, B, C, Z> {

    private final BuilderUtils.ChainingEval<TaskBuilder.F3<A, B, C, ?>> evaluator;
    private final BuilderUtils.ChainingEval<TaskBuilder.F3<A, B, C, Value<?>>> valEvaluator;

    Builder3(
        TaskBuilder.F0<List<Task<?>>> inputs,
        TaskId taskId,
        Class<Z> type,
        BuilderUtils.ChainingEval<TaskBuilder.F3<A, B, C, ?>> evaluator,
        BuilderUtils.ChainingEval<TaskBuilder.F3<A, B, C, Value<?>>> valEvaluator) {
      super(inputs, taskId, type);
      this.evaluator = evaluator;
      this.valEvaluator = valEvaluator;
    }

    @Override
    public Task<Z> process(TaskBuilder.F3<A, B, C, Z> code) {
      return Task.create(inputs, type, evaluator.enclose(code), taskId);
    }

    @Override
    public Task<Z> processWithContext(TaskBuilder.F4<TaskContext, A, B, C, Value<Z>> code) {
      EvalClosure<Z> closure =
          tc -> valEvaluator.<Z>enclose((a, b, c) -> code.apply(withId(tc, taskId), a, b, c)).eval(tc);
      return Task.create(inputs, type, closure, taskId);
    }
  }
}
