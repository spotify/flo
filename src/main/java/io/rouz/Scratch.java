package io.rouz;

import com.google.auto.value.AutoValue;

import java.util.List;
import java.util.function.Function;

import static java.util.Collections.emptyList;

/**
 *
 * Task definitions have (TD)
 *  - a unique name
 *  - a list of parameters of specific types
 *  - an output type
 *
 * Task instances have (TI)
 *  - a TD
 *  - specific values for all TD parameters
 *  - a list of input TI
 *  - code for producing the output
 *
 *
 * Notes
 *  - the tricky part seems to lie in propagating depended task results to the dependent task
 *  --- main issue is in type safety, can easily be done with hashmaps
 *  - creating task instances should only yield a full dependency tree based on the task parameters
 *  - execution of tasks is subject to memoization
 *
 *  - can the setup, with dynamic dependencies be done with type-safe code?
 *  --- maybe with a value+trait based approach rather than classes+inheritance
 *
 *  - facts matching as basis for dependency satisfaction
 */
public class Scratch {

  public static void main(String[] args) {
    Task<String> task1 = MyTask.create("foobarbaz");
    Task<Integer> task2 = Adder.create(5, 7);

    System.out.println("task1.output() = " + task1.output(null));
    System.out.println("task2.output() = " + task2.output(null));
  }

  static class MyTask {
    static final int PLUS = 10;

    static Task<String> create(String parameter) {
      return Tasks
          .in(Adder.create(parameter.length(), PLUS))
          .in(Fib.create(parameter.length()))
          .process(context -> sum -> fib -> something(parameter, sum, fib));
    }

    static String something(String parameter, int sum, int fib) {
      return "len('" + parameter + "') + " + PLUS + " = " + sum + ", btw fib(len) = " + fib;
    }
  }

  static class Adder {
    static Task<Integer> create(int a, int b) {
      return Tasks.create(context -> a + b);
    }
  }

  static class Fib {
    static Task<Integer> create(int n) {
      System.out.print("Fib.create(" + n + ")");
      if (n < 2) {
        System.out.println(".");
        return Tasks
            .create(context -> 1);
      } else {
        System.out.println("");
        return Tasks
            .in(Fib.create(n - 1))
            .in(Fib.create(n - 2))
            .process(context -> a -> b -> {
                       System.out.println("Fib.process(" + a + ", " + b + ")");
                       return a + b;
                     });
      }
    }
  }

  @AutoValue
  static abstract class Tasks<T> implements Task<T> {

    protected abstract Function<TaskContext, T> code();

    static <A> TaskBuilder1<A> in(Task<A> aTask) {
      return new TaskBuilder1<A>() {
        @Override
        public <R> Task<R> process(Function<TaskContext, F1<A, R>> code) {
          return create(
              c -> code
                  .apply(c)
                  .apply(aTask.output(c)));
        }

        @Override
        public <B> TaskBuilder2<A, B> in(Task<B> bTask) {
          return new TaskBuilder2<A, B>() {
            @Override
            public <R> Task<R> process(Function<TaskContext, F2<A, B, R>> code) {
              return create(
                  c -> code
                      .apply(c)
                      .apply(aTask.output(c))
                      .apply(bTask.output(c)));
            }

            @Override
            public <C> TaskBuilder3<A, B, C> in(Task<C> cTask) {
              return new TaskBuilder3<A, B, C>() {
                @Override
                public <R> Task<R> process(Function<TaskContext, F3<A, B, C, R>> code) {
                  return create(
                      c -> code
                          .apply(c)
                          .apply(aTask.output(c))
                          .apply(bTask.output(c))
                          .apply(cTask.output(c)));
                }
              };
            }
          };
        }
      };
    }

    static <T> Task<T> create(Function<TaskContext, T> code) {
      return new AutoValue_Scratch_Tasks<>(emptyList(), emptyList(), code);
    }

    @Override
    public T output(TaskContext taskContext) {
      return code().apply(taskContext);
    }
  }

  interface TaskBuilder1<A> {
    <R> Task<R> process(Function<TaskContext, F1<A, R>> code);
    <B> TaskBuilder2<A, B> in(Task<B> task);
  }

  interface TaskBuilder2<A, B> {
    <R> Task<R> process(Function<TaskContext, F2<A, B, R>> code);
    <C> TaskBuilder3<A, B, C> in(Task<C> task);
  }

  interface TaskBuilder3<A, B, C> {
    <R> Task<R> process(Function<TaskContext, F3<A, B, C, R>> code);
  }

  interface F1<A, R> extends Function<A, R> {}
  interface F2<A, B, R> extends Function<A, Function<B, R>> {}
  interface F3<A, B, C, R> extends Function<A, Function<B, Function<C, R>>> {}

  interface Task<T> {
    List<Parameter> parameters();
    List<Task<?>> inputs();

    T output(TaskContext taskContext);
  }

  interface Parameter {
  }

  interface TaskContext {
    // should contain {@link Output}s of each input task
  }
}
