package io.rouz.flo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collector;
import java.util.stream.Stream;

import io.rouz.flo.context.AsyncContext;
import io.rouz.flo.context.InMemImmediateContext;

/**
 * A context for controlling {@link Task} evaluation and {@link Value} computation.
 */
public interface TaskContext {

  Logger LOG = LoggerFactory.getLogger(TaskContext.class);

  /**
   * The entry point for evaluating a {@link Value} from a {@link Task}.
   *
   * <p>All upstreams to the task will also be evaluated through this same method.
   *
   * <p>The implementation should define how and where evaluation happens.
   *
   * <p>This method is a good place to define how for instance value memoization happens.
   *
   * @param task  The task to evaluate
   * @param <T>   The type of the task result
   * @return A value of the task result
   */
  default <T> Value<T> evaluate(Task<T> task) {
    return evaluateInternal(task, this);
  }

  /**
   * A variant of {@link #evaluate(Task)} that allows the caller to specify the {@link TaskContext}
   * that should be used within the graph during evaluation.
   *
   * <p>This is intended to be called from {@link TaskContext} implementations that form a
   * composition of other contexts.
   *
   * @param task     The task to evaluate
   * @param context  The context to use in further evaluation
   * @param <T>      The type of the task result
   * @return A value of the task result
   */
  default <T> Value<T> evaluateInternal(Task<T> task, TaskContext context) {
    return task.code().eval(context);
  }

  /**
   * Invoke the process function of a task.
   *
   * <p>This method will be called when the process function of a task is ready to be invoked. This
   * gives this {@link TaskContext} the responsibility of invoking user code. By overriding this
   * method, one can intercept the evaluation flow just at the moment between inputs being ready
   * and when the user supplied function for task processing is being invoked.
   *
   * <p>The default implementation will simply invoke the function immediately.
   *
   * <p>Interception of curried process functions will gate the innermost function, while
   * for regular arity 1-3 process functions gating happens around the whole function.
   *
   * todo: reconsider difference between arity and curried function interception.
   *
   * @param taskId     The id of the task being invoked
   * @param processFn  A lazily evaluated handle to the process function
   * @param <T>        The task value type
   * @return The value of the process function invocation
   */
  default <T> Value<T> invokeProcessFn(TaskId taskId, Fn<Value<T>> processFn) {
    return processFn.get();
  }

  /**
   * When called from within any of the functions passed to {@code processWithContext}, this
   * method will return the {@link TaskId} of the task being processed. Otherwise an empty value
   * will be returned.
   *
   * <p>The return value of this method is stable for each instance of {@link TaskContext} that is
   * passed into the process functions. Calls from multiple threads will see the same result as
   * longs as the calls are made to the same instance.
   *
   * @return The id of the task that is being evaluated or empty if called from outside of a
   * process function
   */
  default Optional<TaskId> currentTaskId() {
    return Optional.empty();
  }

  /**
   * Create a {@link Value} with semantics defined by this {@link TaskContext}
   *
   * @param value  A value value supplier
   * @param <T>    The type of the value
   * @return A value with added semantics
   */
  <T> Value<T> value(Fn<T> value);

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
   * Create a promise for a value that can be fulfilled somewhere else.
   *
   * @param <T>  The type of the promised value
   * @return A promise
   */
  <T> Promise<T> promise();

  /**
   * A {@link Collector} that collects a {@link Stream} of {@link Value}s into a {@link Value}
   * of a {@link List}.
   *
   * <p>The semantics of joining {@link Value}s is decided by this {@link TaskContext}.
   *
   * @param <T>  The inner type of the values
   * @return A collector for a stream of values
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
   * {@link TaskContext#value(Fn)}, {@link Value#flatMap(Function)} and
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
     * Consume any error the occurred while constructing the enclosed value.
     *
     * @param errorConsumer  The code that should consume the error
     */
    void onFail(Consumer<Throwable> errorConsumer);

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
     * <p>The returned other value could be from a different {@link TaskContext} so the implementor
     * of this context should take care of how to bridge the value semantics to the returned value.
     *
     * @param fn   The function to map the enclosed value through
     * @param <U>  The type of the new enclosed value
     * @return A new value with a different type
     */
    <U> Value<U> flatMap(Function<? super T, ? extends Value<? extends U>> fn);
  }

  /**
   * A promise for a {@link Value} that is supposed to be {@link #set(Object)}.
   *
   * <p>This is supposed to be used where the processing for producing a value happens in a
   * different environment but is needed by values produced in this context.
   *
   * @param <T>  The type of the promised value
   */
  interface Promise<T> {

    /**
     * The value for this promise. When this promise is fulfilled, the value will become available.
     *
     * @return The value corresponding to this promise
     */
    Value<T> value();

    /**
     * Fulfill the promise.
     *
     * @param value  The value to fulfill the promise with
     * @throws IllegalStateException if the promise was already fulfilled
     */
    void set(T value);

    /**
     * Fail the promise.
     *
     * @param throwable  The exception that is the cause of the failure
     */
    void fail(Throwable throwable);
  }

  /**
   * Create a default, in-memory, immediate {@link TaskContext}. The values produced by this
   * context should behave like synchronously created values. All graph memoization is done
   * completely in memory.
   *
   * @return The context
   */
  static TaskContext inmem() {
    return InMemImmediateContext.create();
  }

  /**
   * Create an asynchronous {@link TaskContext} that executes all evaluation on the given
   * {@link Executor}.
   *
   * @param executor  The executor to run evaluations on
   * @return The asynchronous context
   */
  static TaskContext async(Executor executor) {
    return AsyncContext.create(executor);
  }
}
