# flo

[![CircleCI](https://img.shields.io/circleci/project/rouzwawi/flo.svg)](https://circleci.com/gh/rouzwawi/flo)
[![Maven Central](https://img.shields.io/maven-central/v/io.rouz/flo.svg)](https://search.maven.org/#search%7Cga%7C1%7Cg%3A%22io.rouz%22%20flo*)
[![License](https://img.shields.io/github/license/rouzwawi/flo.svg)](LICENSE.txt)

> `flo` is under development and might not yet live up to all the listed features.

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

## Dependency

```xml
<dependency>
  <groupId>io.rouz</groupId>
  <artifactId>flo-workflow</artifactId>
  <version>0.0.1</version>
</dependency>
```

## Example: Fibonacci

```java
class Fib {

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
    Task<Long> fib92 = nth(92);
    System.out.println("fib92.out() = " + fib92.out());
  }
}
```

## CLI generator

> `flo-cli` is in an experimental stage of development and not really useful at this time.

```xml
<dependency>
  <groupId>io.rouz</groupId>
  <artifactId>flo-cli</artifactId>
  <version>0.0.1</version>
  <optional>true</optional>
</dependency>
```

By adding the `@RootTask` annotation to the `nth(long)` constructor in the Fibonacci example, `flo` will generate a CLI:

```java
class Fib {

  @RootTask
  static Task<Long> nth(long n) {
    // ...
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
$ java -jar fib.jar create Fib.nth -n 92

task.id() = Fib(92)#7178b126
task.out() = 7540113804746346429
```
