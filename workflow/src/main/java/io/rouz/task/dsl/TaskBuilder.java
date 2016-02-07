package io.rouz.task.dsl;

import java.io.Serializable;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

import io.rouz.task.Task;
import io.rouz.task.TaskContext;
import io.rouz.task.TaskContext.Value;

/**
 * Types for the fluent task setup API
 *
 * The entry point for this api is {@link Task#named(String, Object...)}.
 *
 * Note, the inner types should never have to explicitly be mentioned or imported. The API is
 * supposed to be used through fluent calls that eventually lead to a {@link Task} instance.
 */
public interface TaskBuilder {

  <R> Task<R> constant(F0<R> code);
  <R> Task<R> processWithContext(F1<TaskContext, Value<R>> code);
  <A> TaskBuilder1<A> in(F0<Task<A>> task);
  <A> TaskBuilder1<List<A>> ins(F0<List<Task<A>>> tasks);

  <Z> TaskBuilderC0<Z> curryTo();
  <Z> TaskBuilderCV0<Z> curryToValue();

  interface TaskBuilderC0<Z> {
    <A> TaskBuilderC<A, Z, Z> in(F0<Task<A>> task);
    <A> TaskBuilderC<List<A>, Z, Z> ins(F0<List<Task<A>>> tasks);
  }

  interface TaskBuilderCV0<Z> {
    <A> TaskBuilderCV<A, Value<Z>, Z> in(F0<Task<A>> task);
    <A> TaskBuilderCV<List<A>, Value<Z>, Z> ins(F0<List<Task<A>>> tasks);
  }

  interface TaskBuilderC<A, Y, Z> {
    Task<Z> process(F1<A, Y> code);
    <B> TaskBuilderC<B, F1<A, Y>, Z> in(F0<Task<B>> task);
    <B> TaskBuilderC<List<B>, F1<A, Y>, Z> ins(F0<List<Task<B>>> tasks);
  }

  interface TaskBuilderCV<A, Y, Z> {
    Task<Z> process(F1<TaskContext, F1<A, Y>> code);
    <B> TaskBuilderCV<B, F1<A, Y>, Z> in(F0<Task<B>> task);
    <B> TaskBuilderCV<List<B>, F1<A, Y>, Z> ins(F0<List<Task<B>>> tasks);
  }

  interface TaskBuilder1<A> {
    <R> Task<R> process(F1<A, R> code);
    <R> Task<R> processWithContext(F2<TaskContext, A, Value<R>> code);
    <B> TaskBuilder2<A, B> in(F0<Task<B>> task);
    <B> TaskBuilder2<A, List<B>> ins(F0<List<Task<B>>> tasks);
  }

  interface TaskBuilder2<A, B> {
    <R> Task<R> process(F2<A, B, R> code);
    <R> Task<R> processWithContext(F3<TaskContext, A, B, Value<R>> code);
    <C> TaskBuilder3<A, B, C> in(F0<Task<C>> task);
    <C> TaskBuilder3<A, B, List<C>> ins(F0<List<Task<C>>> tasks);
  }

  interface TaskBuilder3<A, B, C> {
    <R> Task<R> process(F3<A, B, C, R> code);
    <R> Task<R> processWithContext(F4<TaskContext, A, B, C, Value<R>> code);
  }

  @FunctionalInterface
  interface F0<R> extends Supplier<R>, Serializable {
    R get();
  }

  @FunctionalInterface
  interface F1<A, R> extends Function<A, R>, Serializable {
    R apply(A a);
  }

  @FunctionalInterface
  interface F2<A, B, R> extends BiFunction<A, B, R>, Serializable {
    R apply(A a, B b);
  }

  @FunctionalInterface
  interface F3<A, B, C, R> extends Serializable {
    R apply(A a, B b, C c);
  }

  @FunctionalInterface
  interface F4<A, B, C, D, R> extends Serializable {
    R apply(A a, B b, C c, D d);
  }
}
