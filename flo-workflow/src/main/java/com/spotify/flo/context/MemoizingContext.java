/*-
 * -\-\-
 * Flo Workflow Definition
 * --
 * Copyright (C) 2016 - 2017 Spotify AB
 * --
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * -/-/-
 */

package com.spotify.flo.context;

import static com.spotify.flo.Util.colored;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.spotify.flo.EvalContext;
import com.spotify.flo.Fn;
import com.spotify.flo.Task;
import com.spotify.flo.TaskId;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A flo {@link EvalContext} that allows task types to define custom memoization strategies.
 *
 * <p>This context can be used to extend the {@link EvalContext#evaluate(Task)} algorithm with
 * memoization behaviours specific to the {@link Task#type()}. This is specially useful when a
 * type is backed by a an out-of-process store of some sort (database, service, etc).
 *
 * <p>By returning a value from {@link Memoizer#lookup(Task)}, this context will stop further
 * evaluation of that tasks upstreams and short-circuit the evaluation algorithm at that task node.
 * The resolved value will be used as input to dependent tasks
 *
 * <p>Example
 *
 * <pre>{@code
 *   Tasks a, b and c depend on each other in a chain: a -> b -> c.
 *   // a depends on b, b depends on c
 *
 *   When calling context.evaluate(a), this will be the sequence of calls made:
 *   context.evaluate(a)
 *   aMemoizer.lookup(a) => empty()
 *   context.evaluate(b)
 *   bMemoizer.lookup(b) => empty()
 *   context.evaluate(c)
 *   cMemoizer.lookup(c) => empty()
 *   cValue = context.invokeProcessFn(c, c.fn)
 *   cMemoizer.store(c, cValue)
 *   bValue = context.invokeProcessFn(b, b.fn)
 *   bMemoizer.store(b, bValue)
 *   aValue = context.invokeProcessFn(a, a.fn)
 *   aMemoizer.store(a, aValue)
 *
 *   However, if any of the lookup calls return a value, the evaluation will short-circuit:
 *   context.evaluate(a)
 *   aMemoizer.lookup(a) => empty()
 *   context.evaluate(b)
 *   bMemoizer.lookup(b) => 'foo'
 *   // no expansion of upstreams to b
 *   aValue = context.invokeProcessFn(a, a.fn)
 *   aMemoizer.store(a, aValue)
 * }</pre>
 *
 * <p>{@link Memoizer} implementations are discovered through the {@link Memoizer.Impl} annotations
 * on a static method on the task type that should have the signature
 * {@code public static Memoizer<T> memoizer()}. The {@link Memoizer} type argument there should
 * match the memoized type itself.
 */
public class MemoizingContext extends ForwardingEvalContext {

  private static final Logger LOG = LoggerFactory.getLogger(MemoizingContext.class);

  public interface Memoizer<T> {

    /**
     * Marks a method with signature {@code public static Memoizer<T> memoizer()}.
     *
     * <p>Can be called multiple times during an evaluation. The memoizer instance must not itself
     * contain the memoizing context, but rather use an internal registry or store to keep track
     * of stored values.
     */
    @Target(ElementType.METHOD)
    @Retention(RetentionPolicy.RUNTIME)
    @interface Impl { }

    /**
     * Lookup a memoized value for a given task. Implementation can throw out a {@link RuntimeException}
     * and that would fail the task evaluation.
     *
     * @param task  The task for which the lookup is made
     * @return An optional memoized value for the task
     */
    Optional<T> lookup(Task<T> task);

    /**
     * Store an evaluated value for a given task. Implementation can throw out a {@link RuntimeException}
     * and that would fail the task evaluation.
     *
     * @param task   The task for which the value was produced
     * @param value  The value that was produced
     */
    void store(Task<T> task, T value);

    /**
     * A memoizer that does nothing and always returns empty {@link #lookup} values
     */
    static <T> Memoizer<T> noop() {
      //noinspection unchecked
      return (Memoizer<T>) NOOP;
    }
  }

  private static final Memoizer<Object> NOOP = new Memoizer<Object>() {

    @Override
    public Optional<Object> lookup(Task<Object> task) {
      return Optional.empty();
    }

    @Override
    public void store(Task<Object> task, Object value) {
    }
  };

  private final ImmutableMap<Class<?>, Memoizer<?>> memoizers;
  private final ConcurrentMap<TaskId, EvalBundle<?>> ongoing = Maps.newConcurrentMap();

  private MemoizingContext(EvalContext baseContext, ImmutableMap<Class<?>, Memoizer<?>> memoizers) {
    super(baseContext);
    this.memoizers = Objects.requireNonNull(memoizers);
  }

  public static EvalContext composeWith(EvalContext baseContext) {
    return builder(baseContext).build();
  }

  public static Builder builder(EvalContext baseContext) {
    return new Builder(baseContext);
  }

  public static class Builder {

    private final EvalContext baseContext;
    private final ImmutableMap.Builder<Class<?>, Memoizer<?>> memoizers = ImmutableMap.builder();

    public Builder(EvalContext baseContext) {
      this.baseContext = Objects.requireNonNull(baseContext);
    }

    /**
     * Add an explicit memoizer for some type
     */
    public <T> Builder memoizer(Memoizer<T> memoizer) {
      mapMemoizer(memoizer);
      return this;
    }

    public EvalContext build() {
      return new MemoizingContext(baseContext, memoizers.build());
    }

    private void mapMemoizer(Memoizer<?> memoizer) {
      for (Type iface : memoizer.getClass().getGenericInterfaces()) {
        if (iface.getTypeName().contains(Memoizer.class.getTypeName())) {
          final ParameterizedType paramType = (ParameterizedType) iface;
          final Class<?> memoizedType = (Class<?>) paramType.getActualTypeArguments()[0];
          memoizers.put(memoizedType, memoizer);
        }
      }
    }
  }

  @Override
  public <T> Value<T> evaluateInternal(Task<T> task, EvalContext context) {
    final EvalBundle<?> bundle = ongoing.computeIfAbsent(task.id(), createBundle(task, context));

    bundle.evaluate();

    //noinspection unchecked
    return (Value<T>) bundle.promise.value();
  }

  private <T> Function<TaskId, EvalBundle<T>> createBundle(Task<T> task, EvalContext context) {
    return (ˍ) -> {
      final Memoizer<T> memoizer = findMemoizer(task.type()).orElse(Memoizer.noop());
      return new EvalBundle<>(task, context, memoizer);
    };
  }

  @Override
  public <T> Value<T> invokeProcessFn(TaskId taskId, Fn<Value<T>> processFn) {
    final EvalBundle<T> evalBundle = lookupBundle(taskId);
    final Task<T> task = evalBundle.task;
    final Memoizer<T> memoizer = evalBundle.memoizer;

    return delegate.invokeProcessFn(taskId, () -> processFn.get().map(v -> {
      memoizer.store(task, v);
      return v;
    }));
  }

  /**
   * Lookup existing promise for a given {@link TaskId}.
   *
   * <p>Assumes that the promise either exist or will exist very shortly. The lookup will spin on
   * the map until the promise shows up.
   *
   * <p>This method is needed to overcome a race between the events:
   * 1. task evaluation is asynchronously started
   * 2. the designated promise is put into the map
   * 3. the task process function being invoked
   *
   * <p>Step 2 and 3 can happen in any order.
   *
   * <p>See {@link #createBundle(Task, EvalContext)} and {@link #invokeProcessFn(TaskId, Fn)}.
   *
   * @param taskId  Task id for which to lookup
   * @param <T>     The promise type
   * @return The promise corresponding to the task id
   */
  private <T> EvalBundle<T> lookupBundle(TaskId taskId) {
    EvalBundle<T> spin;
    do {
      //noinspection unchecked
      spin = (EvalBundle<T>) ongoing.get(taskId);
    } while (spin == null);
    return spin;
  }

  private <T> Optional<Memoizer<T>> findMemoizer(Class<T> type) {
    //noinspection unchecked
    final Optional<Memoizer<T>> tMemoizer = Optional.ofNullable((Memoizer<T>) memoizers.get(type));

    return Optional.ofNullable(tMemoizer.orElseGet(() -> {
      for (Method method : type.getDeclaredMethods()) {
        if (method.getDeclaredAnnotation(Memoizer.Impl.class) != null) {
          //noinspection unchecked
          return (Memoizer<T>) invokeAndPropagateException(method);
        }
      }
      return null;
    }));
  }

  private static Object invokeAndPropagateException(Method method, Object... args) {
    try {
      return method.invoke(/* static */ null, args);
    } catch (IllegalAccessException | InvocationTargetException e) {
      throw Throwables.propagate(e);
    }
  }

  private static <T> void chain(Value<T> value, Promise<T> promise) {
    value.consume(promise::set);
    value.onFail(promise::fail);
  }

  private final class EvalBundle<T> {

    private final Task<T> task;
    private final Promise<T> promise;
    private final EvalContext context;
    private final Memoizer<T> memoizer;

    private volatile boolean evaluated = false;

    private EvalBundle(Task<T> task, EvalContext context, Memoizer<T> memoizer) {
      this.task = task;
      this.context = context;
      this.memoizer = memoizer;

      this.promise = context.promise();
    }

    synchronized void evaluate() {
      if (evaluated) {
        return;
      }
      evaluated = true;

      final Optional<T> lookup = memoizer.lookup(task);
      if (lookup.isPresent()) {
        final T t = lookup.get();
        LOG.debug("Not expanding {}, lookup = {}", colored(task.id()), t);
        promise.set(t);
      } else {
        LOG.debug("Expanding {}", colored(task.id()));
        chain(delegate.evaluateInternal(task, context), promise);
      }
    }
  }
}
