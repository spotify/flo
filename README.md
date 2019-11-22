# ![flo](http://spotify.github.io/flo/doc/flo-logo.svg) &nbsp;&nbsp; [![CircleCI](https://circleci.com/gh/spotify/flo/tree/master.svg?style=svg)](https://circleci.com/gh/spotify/flo/tree/master) [![Codecov](https://img.shields.io/codecov/c/github/spotify/flo/master.svg?maxAge=2592000)](https://codecov.io/github/spotify/flo) [![Maven Central](https://img.shields.io/maven-central/v/com.spotify/flo.svg)](https://search.maven.org/#search%7Cga%7C1%7Cg%3A%22com.spotify%22%20flo*) [![License](https://img.shields.io/github/license/spotify/flo.svg)](LICENSE.txt)

**Please note that we, at Spotify, have ceased further development of flo, so no new features will come;
on the other hand, we will fix critical issues.**

![flo](http://spotify.github.io/flo/doc/flo-logo-small.svg) is a lightweight workflow definition library

* It's not a workflow management system
* It's not a workflow scheduler

__Some key features__

* Programmatic Java and Scala API for expressing workflow construction (_task [DAG] expansion_)
* Use of arbitrary program logic for DAG expansion
* Recursive definitions
* Lazy DAG expansion
* DAG serialization (for 3rd party persistence)
* Extensible DAG evaluation

## Dependency

```xml
<dependency>
  <groupId>com.spotify</groupId>
  <artifactId>flo-workflow</artifactId>
  <version>${flo.version}</version>
</dependency>
```

```sbt
"com.spotify" %% "flo-scala" % floVersion
```

JavaDocs here: http://spotify.github.io/flo/maven/latest/apidocs/

## Table of contents

- [Quick Example: Fibonacci](#quick-example-fibonacci)
- [`Task<T>`](#taskt)
  - [Tasks are defined by regular methods](#tasks-are-defined-by-regular-methods)
    - [Task embedding](#task-embedding)
  - [Tasks are lazy](#tasks-are-lazy)
  - [Task DAGs as data structures](#task-dags-as-data-structures)
- [`EvalContext`](#evalcontext)

## Quick Example: Fibonacci

Fibonacci serves as a good example even though it's not at all the kind of thing that `flo` is
meant to be used for. Nevertheless, it demonstrates how a task DAG can be recursively defined with
arbitrary logic governing which inputs are chosen.

```java
class Fib {

  static Task<Long> fib(long n) {
    TaskBuilder<Long> builder = Task.named("fib", n).ofType(Long.class);
    if (n < 2) {
      return builder
          .process(() -> n);
    } else {
      return builder
          .input(() -> fib(n - 1))
          .input(() -> fib(n - 2))
          .process((a, b) -> a + b);
    }
  }

  public static void main(String[] args) {
    Task<Long> fib92 = fib(92);
    EvalContext evalContext = MemoizingContext.composeWith(EvalContext.sync());
    EvalContext.Value<Long> value = evalContext.evaluate(fib92);

    value.consume(f92 -> System.out.println("fib(92) = " + f92));
  }
}
```

Scala equivalent

```scala
import java.util.function.Consumer

import com.spotify.flo._
import com.spotify.flo.context.MemoizingContext

object Fib extends App {

  def fib(n: Long): Task[Long] = defTask[Long](n) dsl (
    if (n < 2) {
      $ process n
    } else {
      $ input fib(n - 1) input fib(n - 2) process (_ + _)
    }
  )

  val fib92 = fib(92)
  val evalContext = MemoizingContext.composeWith(EvalContext.sync)
  val value = evalContext.evaluate(fib92)

  value.consume(new Consumer[Long] {
    //noinspection ScalaStyle
    override def accept(t: Long): Unit = Console.println(s"fib(92) = ${t}")
  })
}
```

For more details on a high-level runner implementation, see [`flo-runner`][flo-runner].

# [`Task<T>`][Task]

[`Task<T>`][Task] is one of the more central types in `flo`. It represents some task which will
evaluate a value of type `T`. It has a parameterized name, zero or more input tasks and a
processing function which will be executed when inputs are evaluated. Tasks come with a few key
properties governing how they are defined, behave and are interacted with. We'll cover these in the
following sections.

## Tasks are defined by regular methods

Your workflow tasks are not defined as classes that extend [`Task<T>`][Task], rather they are
defined by using the `TaskBuilder` API as we've already seen in the fibonacci example. This is in
many ways very similar to a very clean class with no mutable state, only final members and two
overridden methods for inputs and evaluation function. But with a very important difference, we're
handling the input tasks in a type-safe manner. Each input task you add will further construct the
type for your evaluation function. This is how we can get a clean lambda such as `(a, b) -> a + b`
as the evaluation function for our fibonacci example.

Here's a simple example of a `flo` task depending on two other tasks:

```java
Task<Integer> myTask(String arg) {
  return Task.named("MyTask", arg).ofType(Integer.class)
      .input(() -> otherTask(arg))
      .input(() -> yetATask(arg))
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
    // lose all type safety and guess your inputs
    // ...
  }
}
```

### Task embedding

There's of course nothing stopping you from having the task defined in a regular class. It might
even be useful if your evaluation function is part of an existing class. `flo` does not force
anything on to your types, it just needs to know what to run.

```java
class SomeExistingClass {

  private final String arg;

  SomeExistingClass(String arg) {
    this.arg = arg;
  }

  Task<Integer> task() {
    return Task.named("EmbeddedTask", arg).ofType(Integer.class)
        .input(() -> otherTask(arg))
        .input(() -> yetATask(arg))
        .process(this::process);
  }

  int process(String otherResult, int yetAResult) {
    // ...
  }
}
```

## Tasks are lazy

Creating instances of `Task<T>` is cheap. No matter how complex and deep the task DAG might be,
creating the top level `Task<T>` will not cause the whole DAG to be created. This is because all
inputs are declared using a `Supplier<T>`, utilizing their properties for deferred evaluation:

```java
someLibrary.maybeNeedsValue(() -> expensiveCalculation());
```

This pattern is on its way to become an idiom for achieving laziness in Java 8. A good example is
the additions to the [Java 8 Logger] class which lets the logger decide if the log line for a
certain log level should be computed or not.

So we can easily create an endlessly recursive task (useless, but illustrative) and still be able
to construct instances of it without having to worry about how complex or resource consuming the
construction might be.

```java
Task<String> endless() {
  return Task.named("Endless").ofType(String.class)
      .input(() -> endless())
      .process((impossible) -> impossible);
}
```

This means that we can always refer to tasks directly by using their definition:

```java
TaskId endlessTaskId = endless().id();
```

## Task DAGs as data structures

A `Task<T>` can be transformed into a data structure where a materialized view of the task DAG is
needed. In this example we have two simple tasks where one is used as the input to the other.

```java
Task<String> first(String arg) {
  return Task.named("First", arg).ofType(String.class)
      .process(() -> "hello " + arg);
}

Task<String> second(String arg) {
  return Task.named("Second", arg).ofType(String.class)
      .input(() -> first(arg))
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

The `id` and `inputs` fields should be pretty self explanatory. `isReference` is a boolean which
signals if some task has already been materialized earlier in the tree, given a depth first,
post-order traversal.

Recall that the DAG expansion can choose inputs arbitrarily based on the arguments. In workflow
libraries where expansion is coupled with evaluation, it's hard to know what will be evaluated
beforehand. Evaluation planning and result caching/memoizing becomes integral parts of such
libraries. `flo` aims to expose useful information together with flexible evaluation apis to make
it a library for easily building workflow management systems, rather than trying to be the
can-do-it-all workflow management system itself. More about how this is achieved in the
[`EvalContext`][EvalContext] sections.

# [`EvalContext`][EvalContext]

[`EvalContext`][EvalContext] defines an interface to a context in which `Task<T>` instances are
evaluated. The context is responsible for expanding the task DAG and invoking the task
process-functions. It gives library authors a powerful abstraction to use when implementing
the specific details of evaluating a task DAG. All details around setting up wiring of dependencies
between tasks, interaction with user code for DAG expansion, invoking task functions with upstream
arguments, and other mundane plumbing is dealt with by flo.

These are just a few aspects of evaluation that can be implemented in a `EvalContext`:

* Evaluation concurrency and thread pool management
* Persisted memoization of previous task evaluations
* Distributed coordination of evaluating shared DAGs
* Short-circuiting DAG expansion of previously evaluated tasks

Since multi worker, asynchronous evaluation is a very common pre-requisite for many evaluation
implementations, flo comes with a base implementation of an [`AsyncContext`][AsyncContext] that
can be extended with further behaviour.

See also [`SyncContext`][SyncContext], [`InstrumentedContext`][InstrumentedContext] and
[`MemoizingContext`][MemoizingContext].

[Task]: http://spotify.github.io/flo/maven/latest/apidocs/com/spotify/flo/Task.html
[EvalContext]: http://spotify.github.io/flo/maven/latest/apidocs/com/spotify/flo/EvalContext.html
[AsyncContext]: http://spotify.github.io/flo/maven/latest/apidocs/com/spotify/flo/context/AsyncContext.html
[SyncContext]: http://spotify.github.io/flo/maven/latest/apidocs/com/spotify/flo/context/SyncContext.html
[InstrumentedContext]: http://spotify.github.io/flo/maven/latest/apidocs/com/spotify/flo/context/InstrumentedContext.html
[MemoizingContext]: http://spotify.github.io/flo/maven/latest/apidocs/com/spotify/flo/context/MemoizingContext.html
[Java 8 Logger]: https://docs.oracle.com/javase/8/docs/api/java/util/logging/Logger.html#finest-java.util.function.Supplier-
[DAG]: https://en.wikipedia.org/wiki/Directed_acyclic_graph
[flo-runner]: https://github.com/spotify/flo/tree/master/flo-runner
