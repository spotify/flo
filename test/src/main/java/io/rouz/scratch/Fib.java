package io.rouz.scratch;

import io.rouz.flo.Task;
import io.rouz.flo.TaskContext;
import io.rouz.flo.TaskBuilder;

/**
 * Simple fibonacci implementations
 */
final class Fib {

  public static void main(String[] args) {
    Task<Long> fib92 = create(92);
    TaskContext taskContext = TaskContext.inmem();
    TaskContext.Value<Long> value = taskContext.evaluate(fib92);

    value.consume(f92 -> System.out.println("fib(92) = " + f92));
  }

  static Task<Long> create(long n) {
    TaskBuilder<Long> fib = Task.named("Fib", n).ofType(Long.class);
    if (n < 2) {
      return fib
          .process(() -> n);
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
