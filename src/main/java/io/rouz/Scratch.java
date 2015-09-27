package io.rouz;

import io.rouz.task.Task;
import io.rouz.task.dsl.TaskBuilder;

/**
 * Task definitions have (TD)
 *  - a unique name
 *  - a list of parameters of specific types
 *  - an output type
 *
 * Task instances have (TI)
 *  - a TD
 *  - specific values for all TD parameters
 *  - a list of lazy input TI
 *  - code for producing the output
 *
 * TI are memoized on their TaskId which is determined only by the parameters.
 * Lazy input TI is essential
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
 *  FIXED: with {@link TaskBuilder} fluent api
 *
 *  - facts matching as basis for dependency satisfaction
 */
public class Scratch {

  public static void main(String[] args) {
    final String input = args.length > 0
        ? args[0]
        : "foobarbaz";
    Task<String> task1 = MyTask.create(input);
    Task<Integer> task2 = Adder.create(5, 7);

    System.out.println("task1.output() = " + task1.output());
    System.out.println("task2.output() = " + task2.output());
  }

  static class MyTask {
    static final int PLUS = 10;

    static Task<String> create(String parameter) {
      return Task.named("MyTask", parameter)
          .in(() -> Adder.create(parameter.length(), PLUS))
          .in(() -> Fib.create(parameter.length()))
          .process((sum, fib) -> something(parameter, sum, fib));
    }

    static String something(String parameter, int sum, int fib) {
      return "len('" + parameter + "') + " + PLUS + " = " + sum + ", btw fib(len) = " + fib;
    }
  }

  static class Adder {
    static Task<Integer> create(int a, int b) {
      return Task.create(() -> a + b, "Adder", a, b);
    }
  }

  static class Fib {
    static Task<Integer> create(int n) {
      if (n < 2) {
        return Task.create(() -> 1, "Fib", n);
      } else {
        return Task.named("Fib", n)
            .in(() -> Fib.create(n - 1))
            .in(() -> Fib.create(n - 2))
            .process(Fib::fib);
      }
    }

    static int fib(int a, int b) {
      System.out.println("Fib.process(" + a + " + " + b + ")");
      return a + b;
    }
  }
}
