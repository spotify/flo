package io.rouz.flo.gen.test;

import io.rouz.task.Task;
import io.rouz.task.TaskContext;
import io.rouz.task.TaskContext.Value;

import java.io.Serializable;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

import javax.annotation.Generated;

@Generated("io.rouz.flo.gen.ApiGeneratorProcessor")
public interface GeneratedApi<Z> {

  Task<Z> process(F0<Z> code);
  Task<Z> processWithContext(F1<TaskContext, TaskContext.Value<Z>> code);

  <A> GeneratedApi1<A, Z> in(F0<Task<A>> task);

  interface GeneratedApi1<A, Z> {
    Task<Z> process(F1<A, Z> code);
    Task<Z> processWithContext(F2<TaskContext, A, Value<Z>> code);

    <B> GeneratedApi2<A, B, Z> in(F0<Task<B>> task);
    <B> GeneratedApi2<A, List<B>, Z> ins(F0<List<Task<B>>> tasks);
  }

  interface GeneratedApi2<A, B, Z> {
    Task<Z> process(F2<A, B, Z> code);
    Task<Z> processWithContext(F3<TaskContext, A, B, Value<Z>> code);

    <C> GeneratedApi3<A, B, C, Z> in(F0<Task<C>> task);
    <C> GeneratedApi3<A, B, List<C>, Z> ins(F0<List<Task<C>>> tasks);
  }

  interface GeneratedApi3<A, B, C, Z> {
    Task<Z> process(F3<A, B, C, Z> code);
    Task<Z> processWithContext(F4<TaskContext, A, B, C, Value<Z>> code);
  }

  @FunctionalInterface
  interface F0<Z> extends Supplier<Z>, Serializable {
    Z get();
  }

  @FunctionalInterface
  interface F1<A, Z> extends Function<A, Z>, Serializable {
    Z apply(A a);
  }

  @FunctionalInterface
  interface F2<A, B, Z> extends BiFunction<A, B, Z>, Serializable {
    Z apply(A a, B b);
  }

  @FunctionalInterface
  interface F3<A, B, C, Z> extends Serializable {
    Z apply(A a, B b, C c);
  }

  @FunctionalInterface
  interface F4<A, B, C, D, Z> extends Serializable {
    Z apply(A a, B b, C c, D d);
  }
}
