package io.rouz.task;

import java.util.List;

/**
 * TODO: document.
 */
public interface CurriedTaskBuilder {

  interface TaskBuilderC0<Z> {
    <A> TaskBuilderC<A, Z, Z> in(Fn<Task<A>> task);
    <A> TaskBuilderC<List<A>, Z, Z> ins(Fn<List<Task<A>>> tasks);
  }

  interface TaskBuilderCV0<Z> {
    <A> TaskBuilderCV<A, TaskContext.Value<Z>, Z> in(Fn<Task<A>> task);
    <A> TaskBuilderCV<List<A>, TaskContext.Value<Z>, Z> ins(Fn<List<Task<A>>> tasks);
  }

  interface TaskBuilderC<A, Y, Z> {
    Task<Z> process(Fn1<A, Y> code);
    <B> TaskBuilderC<B, Fn1<A, Y>, Z> in(Fn<Task<B>> task);
    <B> TaskBuilderC<List<B>, Fn1<A, Y>, Z> ins(Fn<List<Task<B>>> tasks);
  }

  interface TaskBuilderCV<A, Y, Z> {
    Task<Z> process(Fn1<TaskContext, Fn1<A, Y>> code);
    <B> TaskBuilderCV<B, Fn1<A, Y>, Z> in(Fn<Task<B>> task);
    <B> TaskBuilderCV<List<B>, Fn1<A, Y>, Z> ins(Fn<List<Task<B>>> tasks);
  }
}
