package io.rouz.task;

import io.rouz.task.dsl.TaskBuilder;

/**
 * Simple fibonacci implementations
 */
final class Fib {

  public static void main(String[] args) {
    Task<Integer> fib33 = create(33);
    System.out.println("fib33.out() = " + fib33.out());
  }

  static Task<Integer> create(int n) {
    TaskBuilder fib = Task.named("Fib", n);
    if (n < 2) {
      return fib
          .constant(() -> 1);
    } else {
      return fib
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
