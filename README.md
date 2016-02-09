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
  <version>0.0.2</version>
</dependency>
```

JavaDocs here: http://rouz.io/flo/maven/apidocs

## Example: Fibonacci

Fibonacci serves as a good example even though it's not at all the kind of thing that `flo` is meant to be used for. Nevertheless, it demonstrates how a workflow graph can be recursively defined with arbitrary logic governing which inputs are chosen.

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
    TaskContext taskContext = TaskContext.inmem();
    TaskContext.Value<Long> value = taskContext.evaluate(fib92);

    value.consume(f92 -> System.out.println("fib(92) = " + f92));
  }
}
```

## Tasks are lazy

Creating instances of `Task<T>` is cheap. No matter how complex and deep the task graph might be, creating the top level `Task<T>` will not cause the whole graph to be created. This is because all inputs are declared using a `Supplier<T>`, utilizing their properties for deferred evaluation:

```java
someLibrary.maybeNeedsValue(() -> expensiveCalculation());
```

This pattern is on its way to become an idiom for achieve lazyness in Java 8. A good example is the additions to the [Java 8 Logger] class which lets the logger decide if the log line for a certain log level should be computed or not.

So we can easily create an endlessly recursive task (useless, but illustrative) and still be able to construct instances of it without having worry about how complex the graph is.

```java
static Task<String> endless() {
  return Task.named("Endless")
      .in(() -> endless())
      .process((impossible) -> impossible);
}
```

This means that we can always refer to tasks directly by using their definition:

```java
TaskId endlessTaskId = endless().id();
```

[Java 8 Logger]: https://docs.oracle.com/javase/8/docs/api/java/util/logging/Logger.html#finest-java.util.function.Supplier-

## CLI generator

> `flo-cli` is in an experimental stage of development and not really useful at this time.

```xml
<dependency>
  <groupId>io.rouz</groupId>
  <artifactId>flo-cli</artifactId>
  <version>0.0.2</version>
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
