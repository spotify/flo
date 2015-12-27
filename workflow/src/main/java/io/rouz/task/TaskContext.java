package io.rouz.task;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collector;
import java.util.stream.Stream;

/**
 * A context for task evaluation
 *
 * TODO: more documentation
 */
public interface TaskContext {

  Logger LOG = LoggerFactory.getLogger(TaskContext.class);

  default <T> Value<T> evaluate(Task<T> task) {
    final EvalClosure<T> code = task.code();
    return code.eval(this);
  }

  /**
   * Create a {@link Value} with semantics defined by this {@link TaskContext}
   *
   * @param value  An actual value to wrap
   * @param <T>    The type of the value
   * @return A value with added semantics
   */
  <T> Value<T> value(T value);

  /**
   * A {@link Collector} that collects a {@link Stream} of {@link Value}s into a {@link Value}
   * of a {@link List}.
   *
   * The semantics of joining {@link Value}s is decided by this {@link TaskContext}.
   */
  default <T> Collector<Value<T>, ?, Value<List<T>>> toValueList() {
    final Function<List<Value<T>>, Value<List<T>>> finisher = list -> {
      Value<List<T>> values = value(new ArrayList<>());
      for (Value<T> tValue : list) {
        values = values.flatMap(
            l -> tValue.map(
                t -> {
                  l.add(t);
                  return l;
                }
            ));
      }
      return values;
    };

    return Collector.of(
        ArrayList::new,
        List::add,
        (a,b) -> { a.addAll(b); return a; },
        finisher);
  }

  /**
   * A wrapped value with additional semantics for how to run computations.
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

    default <U> Value<U> map(Function<? super T, ? extends U> fn) {
      return flatMap(fn.andThen(context()::value));
    }

    <U> Value<U> flatMap(Function<? super T, ? extends Value<? extends U>> fn);
  }

  /**
   * Get a default, in-memory, immediate {@link TaskContext}. The values produced by this context
   * should behave like synchronously created values. Any graph memoization is done completely
   * in memory.
   *
   * @return The context
   */
  static TaskContext inmem() {
    return new TaskContextImpl();
  }
}
