# flo

[![CircleCI](https://img.shields.io/circleci/project/rouzwawi/flo.svg)](https://circleci.com/gh/rouzwawi/flo)
[![License](https://img.shields.io/github/license/rouzwawi/flo.svg)](LICENSE.txt)

> __disclaimer__, `flo` is under development during my spare time and might not yet live up to all the listed features.

`flo` is a lightweight workflow definition library

* It's not a workflow management system
* It's not a workflow scheduler

__Some key features__

* Programmatic Java and Scala API for expressing workflow construction _(graph expansion)_
* Use of arbitrary program logic for workflow graph expansion
* Recursive definitions
* Lazy workflow graph expansion
* Workflow graph serialization (for 3rd party persistence)
* Extensible workflow graph evaluation
* A command line parser generator for instantiation of workflow definitions: `flo-cli`

## Example: Fibonacci


```java
class Fib {

  @RootTask
  static Task<Long> nth(long n) {
    TaskBuilder fib = Task.named("Fib", n);
    if (n < 2) {
      return fib
          .constant(() -> n);
    } else {
      return fib
          .in(() -> Fib.nth(n - 1))
          .in(() -> Fib.nth(n - 2))
          .process((a, b) -> a + b);
    }
  }

  public static void main(String[] args) throws IOException {
    Cli.forFactories(FloRootTaskFactory.Fib_Nth()).run(args);
  }
}
```

```
$ java -jar fib.jar list
available tasks:

Fib.nth
```

```
$ java -jar fib.jar create Fib.nth -h
Option (* = required)  Description
---------------------  -----------
-h, --help
* -n <Integer: n>
```

```
$ time java -jar fib.jar create Fib.nth -n 92

task.id() = Fib(92)#7178b126
task.out() = 7540113804746346429
0.49s user 0.05s system 178% cpu 0.304 total
```
