package io.rouz.scratch;

import io.rouz.task.Task;
import io.rouz.task.TaskContext;
import io.rouz.task.dsl.TaskBuilder;

/**
 * Simple fibonacci implementations
 */
final class Fib {

  public static void main(String[] args) {
    Task<Long> fib92 = create(92);
    TaskContext context = TaskContext.inmem();
    long out = context.evaluate(fib92);
    System.out.println("out = " + out);
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
