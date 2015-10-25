package io.rouz.task;

import io.rouz.task.dsl.TaskBuilder;
import io.rouz.task.dsl.TaskBuilder.F1;
import io.rouz.task.dsl.TaskBuilder.F2;
import io.rouz.task.dsl.TaskBuilder.F3;
import io.rouz.task.dsl.TaskBuilder.TaskBuilder1;
import io.rouz.task.dsl.TaskBuilder.TaskBuilder2;
import io.rouz.task.dsl.TaskBuilder.TaskBuilder3;

import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Stream.concat;

/**
 * Package local implementation of the {@link TaskBuilder} tree.
 *
 * These classes tackle the exponential growth of paths that can be taken through the
 * {@link TaskBuilder}X interfaces by linearizing the implementation through composing functions.
 *
 * todo: convert all the currier, currier2, currier3 functions to a reader monad
 */
final class TaskBuilders {

  private TaskBuilders() {
  }

  static TaskBuilder rootBuilder(String taskName, Object[] args) {
    return new TB(taskName, args);
  }

  private static class BaseTask {

    protected final String taskName;
    protected final Object[] args;

    protected BaseTask(String taskName, Object[] args) {
      this.taskName = taskName;
      this.args = args;
    }
  }

  @FunctionalInterface
  private interface Currier<F, G> {
    G curried(F code);
  }

  // #############################################################################################

  private static class TB
      extends BaseTask
      implements TaskBuilder {

    TB(String taskName, Object[] args) {
      super(taskName, args);
    }

    @Override
    public <R> Task<R> process(Supplier<R> code) {
      return Task.create(toStream(), tc -> code.get(), taskName, args);
    }

    @Override
    public <A> TaskBuilder1<A> in(Supplier<Task<A>> aTask) {
      return new TB1<>(
          toStream(aTask),
          taskName, args,
          f1 -> tc -> f1.apply(aTask.get().internalOut(tc)));
    }

    @Override
    public <A> TaskBuilder1<List<A>> ins(Supplier<Stream<Task<A>>> aTasks) {
      return new TB1<>(
          toFlatStream(aTasks),
          taskName, args,
          f1 -> tc -> f1.apply(
              aTasks.get().map(t -> t.internalOut(tc)).collect(toList())));
    }
  }

  // #############################################################################################

  private static class TB1<A>
      extends BaseTask
      implements TaskBuilder1<A> {

    private final Stream<? extends Task<?>> tasks;
    private final Currier<F1<A, ?>, F1<TaskContext, ?>> currier;

    TB1(
        Stream<? extends Task<?>> tasks,
        String taskName, Object[] args,
        Currier<F1<A, ?>, F1<TaskContext, ?>> currier) {
      super(taskName, args);
      this.tasks = tasks;
      this.currier = currier;
    }


    @Override
    public <R> Task<R> process(F1<A, R> code) {
      F1<TaskContext, R> f = tc ->
          (R) currier.curried(code).apply(tc);

      return Task.create(
          tasks,
          f::apply,
          taskName, args);
    }

    @Override
    public <B> TaskBuilder2<A, B> in(Supplier<Task<B>> bTask) {
      return new TB2<>(
          concat(tasks, toStream(bTask)),
          taskName, args,
          currier,
          f2 -> (tc, a) ->
              f2.apply(a, bTask.get().internalOut(tc)));
    }

    @Override
    public <B> TaskBuilder2<A, List<B>> ins(Supplier<Stream<Task<B>>> bTasks) {
      return new TB2<>(
          concat(tasks, toFlatStream(bTasks)),
          taskName, args,
          currier,
          f2 -> (tc, a) ->
              f2.apply(a, bTasks.get().map(t -> t.internalOut(tc)).collect(toList())));
    }
  }

  // #############################################################################################

  private static class TB2<A, B>
      extends BaseTask
      implements TaskBuilder2<A, B> {

    private final Stream<? extends Task<?>> tasks;
    private final Currier<F1<A, ?>, F1<TaskContext, ?>> currier;
    private final Currier<F2<A, B, ?>, F2<TaskContext, A, ?>> currier2;

    TB2(
        Stream<? extends Task<?>> tasks,
        String taskName, Object[] args,
        Currier<F1<A, ?>, F1<TaskContext, ?>> currier,
        Currier<F2<A, B, ?>, F2<TaskContext, A, ?>> currier2) {
      super(taskName, args);
      this.tasks = tasks;
      this.currier = currier;
      this.currier2 = currier2;
    }

    @Override
    public <R> Task<R> process(F2<A, B, R> code) {
      F1<TaskContext, R> f = tc -> {
        F2<TaskContext, A, ?> f1 = currier2.curried(code);
        return (R) currier.curried(a -> f1.apply(tc, a)).apply(tc);
      };

      return Task.create(
          tasks,
          f::apply,
          taskName, args);
    }

    @Override
    public <C> TaskBuilder3<A, B, C> in(Supplier<Task<C>> cTask) {
      return new TB3<>(
          concat(tasks, toStream(cTask)),
          taskName, args,
          currier, currier2,
          f3 -> (tc, a, b) ->
              f3.apply(a, b, cTask.get().internalOut(tc)));
    }

    @Override
    public <C> TaskBuilder3<A, B, List<C>> ins(Supplier<Stream<Task<C>>> cTasks) {
      return new TB3<>(
          concat(tasks, toFlatStream(cTasks)),
          taskName, args,
          currier, currier2,
          f3 -> (tc, a, b) ->
              f3.apply(a, b, cTasks.get().map(t -> t.internalOut(tc)).collect(toList())));
    }
  }

  // #############################################################################################

  private static class TB3<A, B, C>
      extends BaseTask
      implements TaskBuilder3<A, B, C> {

    private final Stream<? extends Task<?>> tasks;
    private final Currier<F1<A, ?>, F1<TaskContext, ?>> currier;
    private final Currier<F2<A, B, ?>, F2<TaskContext, A, ?>> currier2;
    private final Currier<F3<A, B, C, ?>, F3<TaskContext, A, B, ?>> currier3;

    TB3(
        Stream<? extends Task<?>> tasks,
        String taskName, Object[] args,
        Currier<F1<A, ?>, F1<TaskContext, ?>> currier,
        Currier<F2<A, B, ?>, F2<TaskContext, A, ?>> currier2,
        Currier<F3<A, B, C, ?>, F3<TaskContext, A, B, ?>> currier3) {
      super(taskName, args);
      this.tasks = tasks;
      this.currier = currier;
      this.currier2 = currier2;
      this.currier3 = currier3;
    }

    @Override
    public <R> Task<R> process(F3<A, B, C, R> code) {
      F1<TaskContext, R> f = tc -> {
        F3<TaskContext, A, B, ?> f2 = currier3.curried(code);
        F2<TaskContext, A, ?> f1 = currier2.curried((a, b) -> f2.apply(tc, a, b));
        return (R) currier.curried(a -> f1.apply(tc, a)).apply(tc);
      };

      return Task.create(
          tasks,
          f::apply,
          taskName, args);
    }
  }

  // #############################################################################################

  /**
   * Converts an array of {@link Supplier}s of {@link Task}s to a {@link Stream} of the same
   * {@link Task}s.
   *
   * It will only evaluate the {@link Task} instances (through calling {@link Supplier#get()})
   * when the returned {@link Stream} is consumed. Thus it retains lazyness.
   *
   * @param tasks  An array of tasks
   * @return A stream of the same tasks
   */
  @SafeVarargs
  private static Stream<Task<?>> toStream(Supplier<? extends Task<?>>... tasks) {
    return Stream.of(tasks).map(Supplier::get);
  }

  @SafeVarargs
  private static Stream<Task<?>> toFlatStream(Supplier<? extends Stream<? extends Task<?>>>... tasks) {
    return Stream.of(tasks).flatMap(Supplier::get);
  }
}
