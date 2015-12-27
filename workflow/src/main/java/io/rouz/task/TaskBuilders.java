package io.rouz.task;

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
      return Task.create(inputs, tc -> tc.value(code.get()), taskName, args);
    }

    @Override
    public <A> TaskBuilder1<A> in(F0<Task<A>> aTask) {
      F0<Task<A>> aTaskSingleton = Singleton.create(aTask);
      return new Builder1<>(
          lazyFlatten(inputs, lazyList(aTaskSingleton)),
          taskName, args,
          f1 -> tc -> tc.evaluate(aTaskSingleton.get())
              .map(f1::apply));
    }

    @Override
    public <A> TaskBuilder1<List<A>> ins(F0<List<Task<A>>> aTasks) {
      F0<List<Task<A>>> aTasksSingleton = Singleton.create(aTasks);
      return new Builder1<>(
          lazyFlatten(inputs, lazyFlatten(aTasksSingleton)),
          taskName, args,
          f1 -> tc -> aTasksSingleton.get()
              .stream().map(tc::evaluate).collect(tc.toValueList())
              .map(f1::apply));
    }

    @Override
    public <Z> TaskBuilderC0<Z> curryTo() {
      return new BuilderC0<>(taskName, args);
    }
  }

  private static class BuilderC0<Z>
      extends BaseRefs<Void>
      implements TaskBuilderC0<Z> {

    BuilderC0(String taskName, Object[] args) {
      super(taskName, args);
    }

    @Override
    public <A> TaskBuilderC<A, Z, Z> in(F0<Task<A>> aTask) {
      F0<Task<A>> aTaskSingleton = Singleton.create(aTask);
      return new BuilderC<>(
          lazyFlatten(inputs, lazyList(aTaskSingleton)),
          taskName, args,
          fn -> tc -> tc.evaluate(aTaskSingleton.get())
              .map(fn::apply));
    }

    @Override
    public <A> TaskBuilderC<List<A>, Z, Z> ins(F0<List<Task<A>>> aTasks) {
      F0<List<Task<A>>> aTasksSingleton = Singleton.create(aTasks);
      return new BuilderC<>(
          lazyFlatten(inputs, lazyFlatten(aTasksSingleton)),
          taskName, args,
          fn -> tc -> aTasksSingleton.get()
              .stream().map(tc::evaluate).collect(tc.toValueList())
              .map(fn::apply));
    }
  }

  // #############################################################################################

  private static class BuilderC<A, Y, Z>
      extends BaseRefs<F1<A, Y>>
      implements TaskBuilderC<A, Y, Z> {

    private BuilderC(
        F0<List<Task<?>>> inputs,
        String taskName, Object[] args,
        Lifter<F1<A, Y>> lifter) {
      super(inputs, lifter, taskName, args);
    }

    @Override
    public Task<Z> process(F1<A, Y> fn) {
      return Task.create(inputs, lifter.liftWithCast(fn), taskName, args);
    }

    @Override
    public <B> TaskBuilderC<B, F1<A, Y>, Z> in(F0<Task<B>> bTask) {
      F0<Task<B>> bTaskSingleton = Singleton.create(bTask);
      return new BuilderC<>(
          lazyFlatten(inputs, lazyList(bTaskSingleton)),
          taskName, args,
          lifter.mapWithContext(
              (tc, fn) -> tc.evaluate(bTaskSingleton.get())
                  .map(fn::apply)));
    }

    @Override
    public <B> TaskBuilderC<List<B>, F1<A, Y>, Z> ins(F0<List<Task<B>>> bTasks) {
      F0<List<Task<B>>> bTasksSingleton = Singleton.create(bTasks);
      return new BuilderC<>(
          lazyFlatten(inputs, lazyFlatten(bTasksSingleton)),
          taskName, args,
          lifter.mapWithContext(
              (tc, fn) -> bTasksSingleton.get()
                  .stream().map(tc::evaluate).collect(tc.toValueList())
                  .map(fn::apply)));
    }
  }

  // #############################################################################################

  private static class Builder1<A>
      extends BaseRefs<F1<A, ?>>
      implements TaskBuilder1<A> {

    Builder1(F0<List<Task<?>>> inputs, String taskName, Object[] args, Lifter<F1<A, ?>> lifter) {
      super(inputs, lifter, taskName, args);
    }

    @Override
    public <R> Task<R> process(F1<A, R> code) {
      return Task.create(inputs, lifter.liftWithCast(code), taskName, args);
    }

    @Override
    public <B> TaskBuilder2<A, B> in(F0<Task<B>> bTask) {
      F0<Task<B>> bTaskSingleton = Singleton.create(bTask);
      return new Builder2<>(
          lazyFlatten(inputs, lazyList(bTaskSingleton)),
          taskName, args,
          lifter.mapWithContext(
              (tc, f2) -> tc.evaluate(bTaskSingleton.get())
                  .map(b -> a -> f2.apply(a, b))));
    }

    @Override
    public <B> TaskBuilder2<A, List<B>> ins(F0<List<Task<B>>> bTasks) {
      F0<List<Task<B>>> bTasksSingleton = Singleton.create(bTasks);
      return new Builder2<>(
          lazyFlatten(inputs, lazyFlatten(bTasksSingleton)),
          taskName, args,
          lifter.mapWithContext(
              (tc, f2) -> bTasksSingleton.get()
                  .stream().map(tc::evaluate).collect(tc.toValueList())
                  .map(b -> a -> f2.apply(a, b))));
    }
  }

  // #############################################################################################

  private static class Builder2<A, B>
      extends BaseRefs<F2<A, B, ?>>
      implements TaskBuilder2<A, B> {

    Builder2(F0<List<Task<?>>> inputs, String taskName, Object[] args, Lifter<F2<A, B, ?>> lifter) {
      super(inputs, lifter, taskName, args);
    }

    @Override
    public <R> Task<R> process(F2<A, B, R> code) {
      return Task.create(inputs, lifter.liftWithCast(code), taskName, args);
    }

    @Override
    public <C> TaskBuilder3<A, B, C> in(F0<Task<C>> cTask) {
      F0<Task<C>> cTaskSingleton = Singleton.create(cTask);
      return new Builder3<>(
          lazyFlatten(inputs, lazyList(cTaskSingleton)),
          taskName, args,
          lifter.mapWithContext(
              (tc, f3) -> tc.evaluate(cTaskSingleton.get())
                  .map(c -> (a, b) -> f3.apply(a, b, c))));
    }

    @Override
    public <C> TaskBuilder3<A, B, List<C>> ins(F0<List<Task<C>>> cTasks) {
      F0<List<Task<C>>> cTasksSingleton = Singleton.create(cTasks);
      return new Builder3<>(
          lazyFlatten(inputs, lazyFlatten(cTasksSingleton)),
          taskName, args,
          lifter.mapWithContext(
              (tc, f3) -> cTasksSingleton.get()
                  .stream().map(tc::evaluate).collect(tc.toValueList())
                  .map(c -> (a, b) -> f3.apply(a, b, c))));
    }
  }

  // #############################################################################################

  private static class Builder3<A, B, C>
      extends BaseRefs<F3<A, B, C, ?>>
      implements TaskBuilder3<A, B, C> {

    Builder3(F0<List<Task<?>>> inputs, String taskName, Object[] args, Lifter<F3<A, B, C, ?>> lifter) {
      super(inputs, lifter, taskName, args);
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

    protected final F0<List<Task<?>>> inputs;
    protected final Lifter<F> lifter;
    protected final String taskName;
    protected final Object[] args;

    protected BaseRefs(String taskName, Object[] args) {
      this(Collections::emptyList, null, taskName, args);
    }

    protected BaseRefs(F0<List<Task<?>>> inputs, Lifter<F> lifter, String taskName, Object[] args) {
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
   * {@link EvalClosure} is the function taking a {@link TaskContext} as an argument.
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
  private interface Lifter<F> extends F1<F, EvalClosure<?>> {

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
    default <G> Lifter<G> mapWithContext(F2<TaskContext, G, Value<F>> mapFn) {
      return g -> tc -> mapFn.apply(tc, g).flatMap(f -> this.apply(f).eval(tc));
    }

    /**
     * Lift a function {@code fn} while casting the return type. This is the only place we do
     * this cast.
     *
     * @param fn   Function to lift
     * @param <R>  The forced return type
     * @return A function from {@link TaskContext} to the forced type
     */
    default <R> EvalClosure<R> liftWithCast(F fn) {
      // force the return type of the lifter function to R
      // not type safe, but isolated to this file
      return (EvalClosure<R>) this.apply(fn);
    }
  }

  /**
   * Converts an array of {@link F0}s of {@link Task}s to a {@link F0} of a list of
   * those tasks {@link Task}s.
   *
   * It will only evaluate the functions (through calling {@link F0#get()})
   * when the returned function is invoked. Thus it retains lazyness.
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
