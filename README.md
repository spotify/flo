# ![flo](http://rouz.io/flo/doc/flo-logo.svg) &nbsp;&nbsp; [![CircleCI](https://img.shields.io/circleci/project/rouzwawi/flo.svg)](https://circleci.com/gh/rouzwawi/flo) [![Codecov](https://img.shields.io/codecov/c/github/rouzwawi/flo.svg)](https://codecov.io/github/rouzwawi/flo) [![Maven Central](https://img.shields.io/maven-central/v/io.rouz/flo.svg)](https://search.maven.org/#search%7Cga%7C1%7Cg%3A%22io.rouz%22%20flo*) [![License](https://img.shields.io/github/license/rouzwawi/flo.svg)](LICENSE.txt)

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
  <version>${flo.version}</version>
</dependency>
```

JavaDocs here: http://rouz.io/flo/maven/latest/apidocs/

## Table of contents

- [Quick Example: Fibonacci](#quick-example-fibonacci)
- [`Task<T>`](#taskt)
  - [Tasks are defined by regular methods](#tasks-are-defined-by-regular-methods)
    - [Task embedding](#task-embedding)
  - [Tasks are lazy](#tasks-are-lazy)
  - [Task graphs as data structures](#task-graphs-as-data-structures)
- [`TaskContext`](#taskcontext)
- [CLI generator](#cli-generator)

## Quick Example: Fibonacci

Fibonacci serves as a good example even though it's not at all the kind of thing that `flo` is meant to be used for. Nevertheless, it demonstrates how a workflow graph can be recursively defined with arbitrary logic governing which inputs are chosen.

```java
class Fib {

  static Task<Long> nth(long n) {
    TaskBuilder fib = Task.named("Fib", n);
    if (n < 2) {
      return fib
          .process(() -> n);
    } else {
      return fib
          .in(() -> Fib.nth(n - 1))
          .in(() -> Fib.nth(n - 2))
          .process((a, b) -> a + b);
    }
  }

  public static void main(String[] args) {
    Task<Long> fib92 = nth(92);
    TaskContext taskContext = TaskContext.inmem();
    TaskContext.Value<Long> value = taskContext.evaluate(fib92);

    value.consume(f92 -> System.out.println("fib(92) = " + f92));
  }
}
```

# [`Task<T>`][Task]

[`Task<T>`][Task] is one of the more central types in `flo`. It represents some task which will evaluate a value of type `T`. It has a parameterized name, zero or more input tasks and a processing function which will be executed when inputs are evaluated. Tasks come with a few key properties governing how they are defined, behave and are interacted with. We'll cover these in the following sections.

## Tasks are defined by regular methods

Your workflow tasks are not defined as classes that extend [`Task<T>`][Task], rather they are defined by using the [`TaskBuilder`][TaskBuilder] API as we've already seen in the fibonacci example. This is in many ways very similar to a very clean class with no mutable state, only final members and two overriden methods for inputs and evaluation function. But with a very important difference, we're handling the input tasks in a type-safe manner. Each input task you add will further construct the type for your evaluation function. This is how we can get a clean lambda such as `(a, b) -> a + b` as the evaluation function for our fibonacci example.

Here's a simple example of a `flo` task depending on two other tasks:

```java
Task<Integer> myTask(String arg) {
  return Task.named("MyTask", arg)
      .in(() -> otherTask(arg))
      .in(() -> yetATask(arg))
      .process((otherResult, yetAResult) -> /* ... */);
}
```

This is how the same thing would typically look like in other libraries:

```java
class MyTask extends Task<Integer> {

  private final String arg;

  MyTask(String arg) {
    super("MyTask", arg);
    this.arg = arg;
  }

  @Override
  public List<? extends Task<?>> inputs() {
    return Arrays.asList(new OtherTask(arg), new YetATask(arg));
  }

  @Override
  public Integer process(List<Object> inputs) {
    // loose all type safety and guess your inputs
    // ...
  }
}
```

### Task embedding

There's of course nothing stopping you from having the task defined in a regular class. It might even be useful if your evaluation function is part of an existing class. `flo` does not force anything on to your types, it just needs to know what to run.

```java
class SomeExistingClass {

  private final String arg;

  SomeExistingClass(String arg) {
    this.arg = arg;
  }

  Task<Integer> task() {
    return Task.named("EmbeddedTask", arg)
        .in(() -> otherTask(arg))
        .in(() -> yetATask(arg))
        .process(this::process);
  }

  int process(String otherResult, int yetAResult) {
    // ...
  }
}
```

## Tasks are lazy

Creating instances of `Task<T>` is cheap. No matter how complex and deep the task graph might be, creating the top level `Task<T>` will not cause the whole graph to be created. This is because all inputs are declared using a `Supplier<T>`, utilizing their properties for deferred evaluation:

```java
someLibrary.maybeNeedsValue(() -> expensiveCalculation());
```

This pattern is on its way to become an idiom for achieving lazyness in Java 8. A good example is the additions to the [Java 8 Logger] class which lets the logger decide if the log line for a certain log level should be computed or not.

So we can easily create an endlessly recursive task (useless, but illustrative) and still be able to construct instances of it without having to worry about how complex or resource consuming the construction might be.

```java
Task<String> endless() {
  return Task.named("Endless")
      .in(() -> endless())
      .process((impossible) -> impossible);
}
```

This means that we can always refer to tasks directly by using their definition:

```java
TaskId endlessTaskId = endless().id();
```

## Task graphs as data structures

A `Task<T>` can be transformed into a data structure where a materialized view of the workflow graph is needed. In this example we have two simple tasks where one is used as the input to the other.

```java
Task<String> first(String arg) {
  return Task.named("First", arg)
      .process(() -> "hello " + arg);
}

Task<String> second(String arg) {
  return Task.named("Second", arg)
      .in(() -> first(arg))
      .process((firstResult) -> "well, " + firstResult);
}

void printTaskInfo() {
  Task<String> task = second("flo");
  TaskInfo taskInfo = TaskInfo.ofTask(task);
  System.out.println("taskInfo = " + taskInfo);
}
```

`taskInfo` in this example will be:

```
taskInfo = TaskInfo {
  id=Second(flo)#375f5234,
  isReference=false,
  inputs=[
    TaskInfo {
      id=First(flo)#65f4e738,
      isReference=false,
      inputs=[]
    }
  ]
}
```

The `id` and `inputs` fileds should be pretty self explanatory. `isReference` is a boolean which signals if some task has already been materialized eariler in the tree, given a depth first, post-order traversal.

Recall that the graph expansion can chose inputs artibrarily based on the arguments. In workflow libraries where expansion is coupled with evaluation, it's hard to know what will be evaluated beforehand. Evaluation planning and result caching/memoizing becomes integral parts of such libraries. `flo` aims to expose useful information together with flexible evaluation apis to make it a library for easily building workflow management systems, rather than trying to be the can-do-it-all workflow management system itself. More about how this is achieved in the [`TaskContext`][TaskContext] sections.

# [`TaskContext`][TaskContext]

tbw

[Task]: http://rouz.io/flo/maven/latest/apidocs/io/rouz/task/Task.html
[TaskBuilder]: http://rouz.io/flo/maven/latest/apidocs/io/rouz/task/dsl/TaskBuilder.html
[TaskContext]: http://rouz.io/flo/maven/latest/apidocs/io/rouz/task/TaskContext.html
[Java 8 Logger]: https://docs.oracle.com/javase/8/docs/api/java/util/logging/Logger.html#finest-java.util.function.Supplier-

# CLI generator

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
