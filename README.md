# flo

> __Dag__ means __Day__ in Swedish

```java
class Fib {
  static Task<Integer> create(int n) {
    TaskBuilder fib = Task.named("Fib", n);
    if (n < 2) {
      return fib
          .process(() -> 1);
    } else {
      return fib
          .in(() -> Fib.create(n - 1))
          .in(() -> Fib.create(n - 2))
          .process(Fib::plus);
    }
  }

  static int plus(int a, int b) {
    return a + b;
  }
}
```
