package io.rouz.task.dsl;

import io.rouz.task.Task;

import java.io.Serializable;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

/**
 * Types for the fluent task setup API
 *
 * The entry point for this api is {@link Task#named(String, Object...)}.
 *
 * Note, the inner types should never have to explicitly be mentioned or imported. The API is
 * supposed to be used through fluent calls that eventually lead to a {@link Task} instance.
 */
public interface TaskBuilder {

  <R> Task<R> process(F0<R> code);
  <A> TaskBuilder1<A> in(F0<Task<A>> task);
  <A> TaskBuilder1<List<A>> ins(F0<Stream<Task<A>>> tasks);

  interface TaskBuilder1<A> {
    <R> Task<R> process(F1<A, R> code);
    <B> TaskBuilder2<A, B> in(F0<Task<B>> task);
    <B> TaskBuilder2<A, List<B>> ins(F0<Stream<Task<B>>> tasks);
  }

  interface TaskBuilder2<A, B> {
    <R> Task<R> process(F2<A, B, R> code);
    <C> TaskBuilder3<A, B, C> in(F0<Task<C>> task);
    <C> TaskBuilder3<A, B, List<C>> ins(F0<Stream<Task<C>>> tasks);
  }

  interface TaskBuilder3<A, B, C> {
    <R> Task<R> process(F3<A, B, C, R> code);
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
}
