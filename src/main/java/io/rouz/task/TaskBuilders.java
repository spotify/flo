package io.rouz.task;

import io.rouz.task.dsl.TaskBuilder;
import io.rouz.task.dsl.TaskBuilder.F0;
import io.rouz.task.dsl.TaskBuilder.F1;
import io.rouz.task.dsl.TaskBuilder.F2;
import io.rouz.task.dsl.TaskBuilder.F3;
import io.rouz.task.dsl.TaskBuilder.TaskBuilder1;
import io.rouz.task.dsl.TaskBuilder.TaskBuilder2;
import io.rouz.task.dsl.TaskBuilder.TaskBuilder3;

import java.util.List;
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

  static TaskBuilder rootBuilder(String taskName, Object[] args) {
    return new Builder0(taskName, args);
  }

  // #############################################################################################

  private static class Builder0
      extends BaseRefs<Void>
      implements TaskBuilder {

    Builder0(String taskName, Object[] args) {
      super(taskName, args);
    }

    @Override
    public <R> Task<R> process(F0<R> code) {
      return Task.create(toStream(), tc -> code.get(), taskName, args);
    }

    @Override
    public <A> TaskBuilder1<A> in(F0<Task<A>> aTask) {
      return new Builder1<>(
          toStream(aTask),
          taskName, args,
          f1 -> tc -> f1.apply(
              aTask.get().internalOut(tc)));
    }

    @Override
    public <A> TaskBuilder1<List<A>> ins(F0<Stream<Task<A>>> aTasks) {
      return new Builder1<>(
          toFlatStream(aTasks),
          taskName, args,
          f1 -> tc -> f1.apply(
              aTasks.get().map(t -> t.internalOut(tc)).collect(toList())));
    }
  }

  // #############################################################################################

  private static class Builder1<A>
      extends BaseRefs<F1<A, ?>>
      implements TaskBuilder1<A> {

    Builder1(Stream<Task<?>> tasks, String taskName, Object[] args, Lifter<F1<A, ?>> lifter) {
      super(tasks, lifter, taskName, args);
    }


    @Override
    public <R> Task<R> process(F1<A, R> code) {
      return Task.create(tasks, lifter.liftWithCast(code), taskName, args);
    }

    @Override
    public <B> TaskBuilder2<A, B> in(F0<Task<B>> bTask) {
      return new Builder2<>(
          concat(tasks, toStream(bTask)),
          taskName, args,
          lifter.mapWithContext(
              (tc, f2) -> a -> f2.apply(
                  a,
                  bTask.get().internalOut(tc))));
    }

    @Override
    public <B> TaskBuilder2<A, List<B>> ins(F0<Stream<Task<B>>> bTasks) {
      return new Builder2<>(
          concat(tasks, toFlatStream(bTasks)),
          taskName, args,
          lifter.mapWithContext(
              (tc, f2) -> a -> f2.apply(
                  a,
                  bTasks.get().map(t -> t.internalOut(tc)).collect(toList()))));
    }
  }

  // #############################################################################################

  private static class Builder2<A, B>
      extends BaseRefs<F2<A, B, ?>>
      implements TaskBuilder2<A, B> {

    Builder2(Stream<Task<?>> tasks, String taskName, Object[] args, Lifter<F2<A, B, ?>> lifter) {
      super(tasks, lifter, taskName, args);
    }

    @Override
    public <R> Task<R> process(F2<A, B, R> code) {
      return Task.create(tasks, lifter.liftWithCast(code), taskName, args);
    }

    @Override
    public <C> TaskBuilder3<A, B, C> in(F0<Task<C>> cTask) {
      return new Builder3<>(
          concat(tasks, toStream(cTask)),
          taskName, args,
          lifter.mapWithContext(
              (tc, f3) -> (a, b) -> f3.apply(
                  a, b,
                  cTask.get().internalOut(tc))));
    }

    @Override
    public <C> TaskBuilder3<A, B, List<C>> ins(F0<Stream<Task<C>>> cTasks) {
      return new Builder3<>(
          concat(tasks, toFlatStream(cTasks)),
          taskName, args,
          lifter.mapWithContext(
              (tc, f3) -> (a, b) -> f3.apply(
                  a, b,
                  cTasks.get().map(t -> t.internalOut(tc)).collect(toList()))));
    }
  }

  // #############################################################################################

  private static class Builder3<A, B, C>
      extends BaseRefs<F3<A, B, C, ?>>
      implements TaskBuilder3<A, B, C> {

    Builder3(Stream<Task<?>> tasks, String taskName, Object[] args, Lifter<F3<A, B, C, ?>> lifter) {
      super(tasks, lifter, taskName, args);
    }

    @Override
    public <R> Task<R> process(F3<A, B, C, R> code) {
      return Task.create(tasks, lifter.liftWithCast(code), taskName, args);
    }
  }

  // #############################################################################################

  private static class BaseRefs<F> {

    protected final Stream<Task<?>> tasks;
    protected final Lifter<F> lifter;
    protected final String taskName;
    protected final Object[] args;

    protected BaseRefs(Stream<Task<?>> tasks, Lifter<F> lifter, String taskName, Object[] args) {
      this.tasks = tasks;
      this.lifter = lifter;
      this.taskName = taskName;
      this.args = args;
    }

    protected BaseRefs(String taskName, Object[] args) {
      this.tasks = null;
      this.lifter = null;
      this.taskName = taskName;
      this.args = args;
    }
  }

  // #############################################################################################

  /**
   * Higher order function that lifts an arbitrary function {@link F} into a function from
   * {@link TaskContext} to an unknown type.
   *
   * @param <F>  The type of the function that can be lifted
   */
  @FunctionalInterface
  private interface Lifter<F> extends F1<F, F1<TaskContext, ?>> {

    /**
     * Maps this {@link Lifter} into a {@link Lifter} of a different function based on a function
     * with the signature of the current {@link F}. The mapping function will run with the
     * {@link TaskContext} argument available to it.
     *
     * Note that since we're mapping functions to new functions, the types of {@code mapFn} are
     * reversed. Essentially, what the mapping function should do is: "given a function {@link G},
     * give me a function {@link F} that I will apply with this {@link Lifter}". That lets us
     * construct a {@link Lifter} for {@link G}.
     *
     * @param mapFn  The mapping function from {@link G} to {@link F}
     * @param <G>    The function type of the new lifter
     * @return A new lifter that can lift functions of type {@link G}
     */
    default <G> Lifter<G> mapWithContext(F2<TaskContext, G, F> mapFn) {
      return g -> tc -> this.apply(mapFn.apply(tc, g)).apply(tc);
    }

    /**
     * Lift a function {@code fn} while casting the return type. This is the only place we do
     * this cast.
     *
     * @param fn   Function to lift
     * @param <R>  The forced return type
     * @return A function from {@link TaskContext} to the forced type
     */
    default <R> F1<TaskContext, R> liftWithCast(F fn) {
      // force the return type of the lifter function to R
      // not type safe, but isolated to this file
      return (F1<TaskContext, R>) this.apply(fn);
    }
  }

  /**
   * Converts an array of {@link F0}s of {@link Task}s to a {@link Stream} of the same
   * {@link Task}s.
   *
   * It will only evaluate the {@link Task} instances (through calling {@link F0#get()})
   * when the returned {@link Stream} is consumed. Thus it retains lazyness.
   *
   * @param tasks  An array of tasks
   * @return A stream of the same tasks
   */
  @SafeVarargs
  private static Stream<Task<?>> toStream(F0<? extends Task<?>>... tasks) {
    return Stream.of(tasks).map(F0::get);
  }

  @SafeVarargs
  private static Stream<Task<?>> toFlatStream(F0<? extends Stream<? extends Task<?>>>... tasks) {
    return Stream.of(tasks).flatMap(F0::get);
  }
}
