package io.rouz.task;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

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
    public <R> Task<R> constant(F0<R> code) {
      return Task.create(inputs, tc -> code.get(), taskName, args);
    }

    @Override
    public <A> TaskBuilder1<A> in(F0<Task<A>> aTask) {
      F0<Task<A>> aTaskSingleton = Singleton.create(aTask);
      return new Builder1<>(
          taskName, args,
          f1 -> tc -> f1.apply(
              aTaskSingleton.get().internalOut(tc)));
    }

    @Override
    public <A> TaskBuilder1<List<A>> ins(F0<List<Task<A>>> aTasks) {
      F0<List<Task<A>>> aTasksSingleton = Singleton.create(aTasks);
      return new Builder1<>(
          taskName, args,
          f1 -> tc -> f1.apply(
              aTasksSingleton.get().stream().map(t -> t.internalOut(tc)).collect(toList())));
    }

    @Override
    public <R> TaskBuilderC0<R> curryTo(Class<R> returnClass) {
      return new BuilderC0<>(taskName, args);
    }
  }

  private static class BuilderC0<R>
      extends BaseRefs<Void>
      implements TaskBuilderC0<R> {

    BuilderC0(String taskName, Object[] args) {
      super(a -> tc -> a, taskName, args);
    }

    @Override
    public <A> TaskBuilderC<F1<A, R>, R> in(F0<Task<A>> aTask) {
      F0<Task<A>> aTaskSingleton = Singleton.create(aTask);
      return new BuilderC<>(
          taskName, args,
          fn -> tc -> fn.apply(
              aTaskSingleton.get().internalOut(tc)));
    }

    @Override
    public <A> TaskBuilderC<F1<List<A>, R>, R> ins(F0<List<Task<A>>> aTasks) {
      F0<List<Task<A>>> aTasksSingleton = Singleton.create(aTasks);
      return new BuilderC<>(
          taskName, args,
          fn -> tc -> fn.apply(
              aTasksSingleton.get().stream().map(t -> t.internalOut(tc)).collect(toList())));
    }
  }

  // #############################################################################################

  private static class BuilderC<F, R>
      extends BaseRefs<F>
      implements TaskBuilderC<F, R> {

    private BuilderC(String taskName, Object[] args, Lifter<F> lifter) {
      super(lifter, taskName, args);
    }

    private BuilderC(F0<List<TaskId>> inputs, String taskName, Object[] args, Lifter<F> lifter) {
      super(inputs, lifter, taskName, args);
    }

    @Override
    public Task<R> process(F fn) {
      return Task.create(inputs, lifter.liftWithCast(fn), "test");
    }

    @Override
    public <A> TaskBuilderC<F1<A, F>, R> in(F0<Task<A>> aTask) {
      F0<Task<A>> aTaskSingleton = Singleton.create(aTask);
      return new BuilderC<>(
          taskName, args,
          lifter.mapWithContext(
              (tc, fn) -> fn.apply(
                  aTaskSingleton.get().internalOut(tc))));
    }

    @Override
    public <A> TaskBuilderC<F1<List<A>, F>, R> ins(F0<List<Task<A>>> aTasks) {
      F0<List<Task<A>>> aTasksSingleton = Singleton.create(aTasks);
      return new BuilderC<>(
          taskName, args,
          lifter.mapWithContext(
              (tc, fn) -> fn.apply(
                  aTasksSingleton.get().stream().map(t -> t.internalOut(tc)).collect(toList()))));
    }
  }

  // #############################################################################################

  private static class Builder1<A>
      extends BaseRefs<F1<A, ?>>
      implements TaskBuilder1<A> {

    Builder1(String taskName, Object[] args, Lifter<F1<A, ?>> lifter) {
      super(lifter, taskName, args);
    }

    @Override
    public <R> Task<R> process(F1<A, R> code) {
      return Task.create(inputs, lifter.liftWithCast(code), taskName, args);
    }

    @Override
    public <B> TaskBuilder2<A, B> in(F0<Task<B>> bTask) {
      F0<Task<B>> bTaskSingleton = Singleton.create(bTask);
      return new Builder2<>(
          taskName, args,
          lifter.mapWithContext(
              (tc, f2) -> a -> f2.apply(
                  a,
                  bTaskSingleton.get().internalOut(tc))));
    }

    @Override
    public <B> TaskBuilder2<A, List<B>> ins(F0<List<Task<B>>> bTasks) {
      F0<List<Task<B>>> bTasksSingleton = Singleton.create(bTasks);
      return new Builder2<>(
          taskName, args,
          lifter.mapWithContext(
              (tc, f2) -> a -> f2.apply(
                  a,
                  bTasksSingleton.get().stream().map(t -> t.internalOut(tc)).collect(toList()))));
    }
  }

  // #############################################################################################

  private static class Builder2<A, B>
      extends BaseRefs<F2<A, B, ?>>
      implements TaskBuilder2<A, B> {

    Builder2(String taskName, Object[] args, Lifter<F2<A, B, ?>> lifter) {
      super(lifter, taskName, args);
    }

    @Override
    public <R> Task<R> process(F2<A, B, R> code) {
      return Task.create(inputs, lifter.liftWithCast(code), taskName, args);
    }

    @Override
    public <C> TaskBuilder3<A, B, C> in(F0<Task<C>> cTask) {
      F0<Task<C>> cTaskSingleton = Singleton.create(cTask);
      return new Builder3<>(
          taskName, args,
          lifter.mapWithContext(
              (tc, f3) -> (a, b) -> f3.apply(
                  a, b,
                  cTaskSingleton.get().internalOut(tc))));
    }

    @Override
    public <C> TaskBuilder3<A, B, List<C>> ins(F0<List<Task<C>>> cTasks) {
      F0<List<Task<C>>> cTasksSingleton = Singleton.create(cTasks);
      return new Builder3<>(
          taskName, args,
          lifter.mapWithContext(
              (tc, f3) -> (a, b) -> f3.apply(
                  a, b,
                  cTasksSingleton.get().stream().map(t -> t.internalOut(tc)).collect(toList()))));
    }
  }

  // #############################################################################################

  private static class Builder3<A, B, C>
      extends BaseRefs<F3<A, B, C, ?>>
      implements TaskBuilder3<A, B, C> {

    Builder3(String taskName, Object[] args, Lifter<F3<A, B, C, ?>> lifter) {
      super(lifter, taskName, args);
    }

    @Override
    public <R> Task<R> process(F3<A, B, C, R> code) {
      return Task.create(inputs, lifter.liftWithCast(code), taskName, args);
    }
  }

  // #############################################################################################

  /**
   * A convenience class for holding some reference. This is only so that we don't have to repeat
   * these declaration in every class above.
   *
   * @param <F>  The function type that is relevant for the builder
   */
  private static class BaseRefs<F> {

    protected final F0<List<TaskId>> inputs;
    protected final Lifter<F> lifter;
    protected final String taskName;
    protected final Object[] args;

    protected BaseRefs(String taskName, Object[] args) {
      this(Collections::emptyList, null, taskName, args);
    }

    protected BaseRefs(Lifter<F> lifter, String taskName, Object[] args) {
      this(Collections::emptyList, lifter, taskName, args);
    }

    protected BaseRefs(F0<List<TaskId>> inputs, Lifter<F> lifter, String taskName, Object[] args) {
      this.inputs = inputs;
      this.lifter = lifter;
      this.taskName = taskName;
      this.args = args;
    }
  }

  // #############################################################################################

  /**
   * Higher order function that lifts an arbitrary function {@link F} into a function from
   * {@link TaskContext} to an unknown type.
   *
   * Because the mix of a fluent task input construction and multiple input type structures, the
   * implementation graph of {@link TaskBuilder} would grow exponentially by the number of
   * arguments. This interface allows us to use a progressive argument construction technique in
   * the implementation, resulting in a linear amount of classes.
   *
   * It is a special case of a Reader Monad that outputs a function taking a {@link TaskContext}.
   * The Reader 'environment' is the function that we want to lift. Because of the special casing,
   * it can have a special 'withReader' function that allows us to change the environment while
   * having access to the {@link TaskContext} argument that will be used by the output function.
   *
   * We implement this special 'withReader' function in {@link #mapWithContext(F2)}.
   *
   * {@code
   *    Haskell: Reader r a
   *
   *    -- regular withReader function
   *    withReader :: (r' -> r) -> Reader r a -> Reader r' a
   *    withReader f m = Reader $ runReader m . f
   *
   *    -- our special case
   *    withReaderValue :: (r' -> a -> r) -> Reader r (a -> b) -> Reader r' (a -> b)
   *    withReaderValue f m = Reader $ \r' a -> (runReader m) (f r' a) a
   * }
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
  private static List<Task<?>> asList(F0<? extends Task<?>>... tasks) {
    return Stream.of(tasks)
        .map(F0::get)
        .collect(toList());
  }

  @SafeVarargs
  private static List<Task<?>> asFlatList(F0<? extends List<? extends Task<?>>>... tasks) {
    return Stream.of(tasks)
        .map(F0::get)
        .flatMap(List::stream)
        .collect(toList());
  }

  @SafeVarargs
  private static List<Task<?>> concat(List<Task<?>>... lists) {
    return Stream.of(lists)
        .flatMap(List::stream)
        .collect(toList());
  }

  private static class Singleton<T> implements F0<T> {

    private final F0<T> supplier;
    private volatile T value;

    private Singleton(F0<T> supplier) {
      this.supplier = Objects.requireNonNull(supplier);
    }

    private static <T> F0<T> create(F0<T> fn) {
      return new Singleton<>(fn);
    }

    @Override
    public T get() {
      if (value == null) {
        synchronized (this) {
          if (value == null) {
            value = supplier.get();
          }
        }
      }
      return value;
    }
  }
}
