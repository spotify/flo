package io.rouz.task.dsl;

import io.rouz.task.Task;

import java.util.List;
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

  <A> TaskBuilder1<A> in(Supplier<Task<A>> task);
  <A> TaskBuilder1<List<A>> ins(Stream<Task<A>> task);
  <R> Task<R> process(Supplier<R> code);

  interface TaskBuilder1<A> {
    <R> Task<R> process(F1<A, R> code);
    <B> TaskBuilder2<A, B> in(Supplier<Task<B>> task);
  }

  interface TaskBuilder2<A, B> {
    <R> Task<R> process(F2<A, B, R> code);
    <C> TaskBuilder3<A, B, C> in(Supplier<Task<C>> task);
  }

  interface TaskBuilder3<A, B, C> {
    <R> Task<R> process(F3<A, B, C, R> code);
  }

  @FunctionalInterface
  interface F1<A, R> {
    R apply(A a);
  }

  @FunctionalInterface
  interface F2<A, B, R> {
    R apply(A a, B b);
  }

  @FunctionalInterface
  interface F3<A, B, C, R> {
    R apply(A a, B b, C c);
  }
}
