package io.rouz.task;

import io.rouz.task.dsl.TaskBuilder;

/**
 * Simple fibonacci implementations
 */
final class Fib {

  public static void main(String[] args) {
    Task<Long> fib33 = create(92);
    System.out.println("fib33.out() = " + fib33.out());
  }

  static Task<Long> create(long n) {
    TaskBuilder fib = Task.named("Fib", n);
    if (n < 2) {
      return fib
          .constant(() -> n);
    } else {
      return fib
          .in(() -> Fib.create(n - 1))
          .in(() -> Fib.create(n - 2))
          .process(Fib::fib);
    }
  }

  static long fib(long a, long b) {
    System.out.println("Fib.process(" + a + " + " + b + ")");
    return a + b;
  }
}
