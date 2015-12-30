package io.rouz.task;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collector;
import java.util.stream.Stream;

import io.rouz.task.context.InMemImmediateContext;
import io.rouz.task.dsl.TaskBuilder.F0;

/**
 * A context for controlling {@link Task} evaluation and {@link Value} computation.
 */
public interface TaskContext {

  Logger LOG = LoggerFactory.getLogger(TaskContext.class);

  /**
   * The entry point for evalating a {@link Value} from a {@link Task}.
   *
   * All upstreams to the task will also be evaluated through this same method.
   *
   * The implementation should define how and where evaluation happens.
   *
   * This method is a good place to define how for instance value memoization happens.
   *
   * @param task  The task to evaluate
   * @param <T>   The type of the task result
   * @return A value of the task result
   */
  default <T> Value<T> evaluate(Task<T> task) {
    return task.code().eval(this);
  }

  /**
   * Create a {@link Value} with semantics defined by this {@link TaskContext}
   *
   * @param value  A value value supplier
   * @param <T>    The type of the value
   * @return A value with added semantics
   */
  <T> Value<T> value(F0<T> value);

  /**
   * Create a {@link Value} with semantics defined by this {@link TaskContext}
   *
   * @param value  An actual value to wrap
   * @param <T>    The type of the value
   * @return A value with added semantics
   */
  default <T> Value<T> immediateValue(T value) {
    return value(() -> value);
  }

  /**
   * A {@link Collector} that collects a {@link Stream} of {@link Value}s into a {@link Value}
   * of a {@link List}.
   *
   * The semantics of joining {@link Value}s is decided by this {@link TaskContext}.
   */
  default <T> Collector<Value<T>, ?, Value<List<T>>> toValueList() {
    return Collector.of(
        ArrayList::new, List::add, (a,b) -> { a.addAll(b); return a; },
        ValueFold.inContext(this));
  }

  /**
   * A wrapped value with additional semantics for how the enclosed value becomes available and
   * how computations on that value are executed.
   *
   * Value is a Monad and the implementor should minimally need to implement
   * {@link TaskContext#value(F0)}, {@link Value#flatMap(Function)} and
   * {@link Value#consume(Consumer)} to get a working context with it's associated value type.
   *
   * @param <T>  The enclosed type
   */
  interface Value<T> {

    /**
     * The {@link TaskContext} that created this value.
     *
     * @return The context
     */
    TaskContext context();

    /**
     * Consume the enclosed value.
     *
     * @param consumer  The code that should consume the value
     */
    void consume(Consumer<T> consumer);

    /**
     * Map the enclosed value through a function and return a {@link Value} enclosing that result.
     *
     * @param fn   The function to map the enclosed value through
     * @param <U>  The type of the new enclosed value
     * @return A new value with a different type
     */
    default <U> Value<U> map(Function<? super T, ? extends U> fn) {
      return flatMap(fn.andThen(context()::immediateValue));
    }

    /**
     * Map the enclosed value through a function that return another {@link Value}.
     *
     * The returned other value could be from a different {@link TaskContext} so the implementor of
     * this context should take care of how to bridge the value semantics to the returned value.
     *
     * @param fn   The function to map the enclosed value through
     * @param <U>  The type of the new enclosed value
     * @return A new value with a different type
     */
    <U> Value<U> flatMap(Function<? super T, ? extends Value<? extends U>> fn);
  }

  /**
   * Get a default, in-memory, immediate {@link TaskContext}. The values produced by this context
   * should behave like synchronously created values. All graph memoization is done completely
   * in memory.
   *
   * @return The context
   */
  static TaskContext inmem() {
    return InMemImmediateContext.create();
  }
}
