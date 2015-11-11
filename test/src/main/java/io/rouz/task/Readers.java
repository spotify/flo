package io.rouz.task;

import io.rouz.task.dsl.TaskBuilder;
import io.rouz.task.dsl.TaskBuilder.F1;
import io.rouz.task.dsl.TaskBuilder.F2;
import io.rouz.task.dsl.TaskBuilder.F3;

final class Readers {

  @FunctionalInterface
  interface Reader<R, A> extends F1<R, A> {

    default <B> Reader<R, B> bind(F1<A, Reader<R, B>> fn) {
      return r -> fn.apply( apply(r) ).apply(r);
    }

    default <S> Reader<S, A> with(F1<S, R> fn) {
      return s -> apply(fn.apply(s));
    }
  }

  static <A, B> Reader<A, B> ret(B b) {
    return _a -> b;
  }

  static <A> Reader<A, A> ask() {
    return a -> a;
  }

  static <A, B, C, R, X> void foo() {
    TaskBuilder.F0<R> f0 = () -> null;
    Reader<TaskBuilder.F0<R>, F1<X, R>> zero =
        ret(x -> f0.get());

    A ay = null;
    Reader<F1<A, R>, F1<X, R>> one =
        zero.with(f1 -> () -> f1.apply(ay));

    B be = null;
    Reader<F2<A, B, R>, F1<X, R>> two =
        one.with(f2 -> (a) -> f2.apply(a, be));

    C ce = null;
    Reader<F3<A, B, C, R>, F1<X, R>> three =
        two.with(f3 -> (a, b) -> f3.apply(a, b, ce));

    // missing X in all of these 'with' calls
  }
}
