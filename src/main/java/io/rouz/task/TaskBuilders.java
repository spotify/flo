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
 * The linearization is implemented by letting the next builder in the chain take a higher-order
 * function {@link Lifter}. This function allows the builder to lift a function with the right
 * number of arguments into a function from {@link TaskContext} to the result. This function can
 * then either be used to terminate the builder into a {@link Task} (see process(...) methods) or
 * to construct a new {@link Lifter} for the next builder in the chain, adding one more argument to
 * the liftable function.
 */
final class TaskBuilders {

  private TaskBuilders() {
  }

  static TaskBuilder rootBuilder(String taskName, Object[] args) {
    return new TB(taskName, args);
  }

  private interface Lifter<F> extends F1<F, F1<TaskContext, ?>> {
    default <R> F1<TaskContext, R> castReturnType(F fn) {
      // force the return type of the lifter function to R
      // not type safe, but isolated to this file
      return (F1<TaskContext, R>) this.apply(fn);
    }
  }

  private static class BaseTask {
    protected final String taskName;

    protected final Object[] args;
    protected BaseTask(String taskName, Object[] args) {
      this.taskName = taskName;
      this.args = args;
    }

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
          f1 -> tc -> f1.apply(
              aTask.get().internalOut(tc)));
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
    private final Lifter<F1<A, ?>> lifter;

    TB1(
        Stream<? extends Task<?>> tasks,
        String taskName, Object[] args,
        Lifter<F1<A, ?>> lifter) {
      super(taskName, args);
      this.tasks = tasks;
      this.lifter = lifter;
    }


    @Override
    public <R> Task<R> process(F1<A, R> code) {
      F1<TaskContext, R> f = lifter.castReturnType(code);

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
          f2 -> tc -> lifter.apply(
              a -> f2.apply(
                  a,
                  bTask.get().internalOut(tc))
          ).apply(tc));
    }

    @Override
    public <B> TaskBuilder2<A, List<B>> ins(Supplier<Stream<Task<B>>> bTasks) {
      return new TB2<>(
          concat(tasks, toFlatStream(bTasks)),
          taskName, args,
          f2 -> tc -> lifter.apply(
              a -> f2.apply(
                  a,
                  bTasks.get().map(t -> t.internalOut(tc)).collect(toList()))
          ).apply(tc));
    }
  }

  // #############################################################################################

  private static class TB2<A, B>
      extends BaseTask
      implements TaskBuilder2<A, B> {

    private final Stream<? extends Task<?>> tasks;
    private final Lifter<F2<A, B, ?>> lifter;

    TB2(
        Stream<? extends Task<?>> tasks,
        String taskName, Object[] args,
        Lifter<F2<A, B, ?>> lifter) {
      super(taskName, args);
      this.tasks = tasks;
      this.lifter = lifter;
    }

    @Override
    public <R> Task<R> process(F2<A, B, R> code) {
      F1<TaskContext, R> f = lifter.castReturnType(code);

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
          f3 -> tc -> lifter.apply(
              (a, b) -> f3.apply(
                  a, b,
                  cTask.get().internalOut(tc))
          ).apply(tc));
    }

    @Override
    public <C> TaskBuilder3<A, B, List<C>> ins(Supplier<Stream<Task<C>>> cTasks) {
      return new TB3<>(
          concat(tasks, toFlatStream(cTasks)),
          taskName, args,
          f3 -> tc -> lifter.apply(
              (a, b) -> f3.apply(
                  a, b,
                  cTasks.get().map(t -> t.internalOut(tc)).collect(toList()))
          ).apply(tc));
    }
  }

  // #############################################################################################

  private static class TB3<A, B, C>
      extends BaseTask
      implements TaskBuilder3<A, B, C> {

    private final Stream<? extends Task<?>> tasks;
    private final Lifter<F3<A, B, C, ?>> lifter;

    TB3(
        Stream<? extends Task<?>> tasks,
        String taskName, Object[] args,
        Lifter<F3<A, B, C, ?>> lifter) {
      super(taskName, args);
      this.tasks = tasks;
      this.lifter = lifter;
    }

    @Override
    public <R> Task<R> process(F3<A, B, C, R> code) {
      F1<TaskContext, R> f = lifter.castReturnType(code);

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
