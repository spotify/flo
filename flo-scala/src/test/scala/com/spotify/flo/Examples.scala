package com.spotify.flo

import java.util.function.Consumer

/**
  * Example task definitions that should compile
  */
object Examples {

  def greet(person: String): Task[String] = defTask(person) process {
    s"hello $person"
  }

  def meet(person: String): Task[String] = defTask(person)
    .in(greet(person))
    .process { (greeting) =>
      s"$greeting, nice to meet you"
    }

  def meetDsl(person: String): Task[String] = defTask[String](person) dsl ($
    in      greet(person)
    process ( greeting =>
      s"$greeting, nice to meet you"
    )
  )

  def world(arg: Int): Task[String] = defTask[String](arg) dsl (
    $ ┬ fib(arg)
      └> (a => s"world $arg:$a")
  )

  def world2(arg: Int): Task[String] = defTaskNamed("custom-name", arg) process {
    s"world $arg"
  }

  def hello: Task[String] = defTask[String]() dsl (
    $ ┬ world(0)
      ╞ List(world(7), world(14))
      ├ world2(21)
      ╞ List(world2(22))
      └> ((a, b, c, d) => s"hello $a $b $c $d")
  )

  def readableDsl: Task[String] = defTask[String]() dsl ($
    // upstream dependencies
    in hello
    in world(7)

    // we'll publish this endpoint when the task is done
    op Publisher("MyEndpoint")

    // run function
    process daFoo
  )

  def daFoo(world: String, wo: String, pub1: Pub): String = {
    pub1.pub("gs://foo/bar")
    "ok"
  }

  def hello2: Task[String] = defTask[String]() dsl ($
    in      world(0)
    ins     List(world(7), world(14))
    in      world2(21)
    ins     List(world2(22))
    process ((a, b, c, d) => s"hello $a $b $c $d")
  )

  def fib(n: Int): Task[Int] = defTask[Int](n) dsl (
    if (n < 2)
      $ -> n
    else
      $ ┬ fib(n - 1)
        ├ fib(n - 2)
        └> (_ + _)
  )

  def fib2(n: Int): Task[Int] = defTask[Int](n) dsl (
    if (n < 2)
      $ process n
    else
      $ in fib2(n - 1)
        in fib2(n - 2)
        process (_ + _)
  )


  def main(args: Array[String]): Unit = {
    EvalContext.sync.evaluate(readableDsl)
      .consume(new Consumer[String] {
        override def accept(t: String) = println(t)
      })
  }

  def print(info: TaskInfo): Unit = {
    println(info.id)
    for (i <- 0 until info.inputs.size()) {
      print(info.inputs.get(i))
    }
  }
}

trait Pub {
  def pub(uri: String): Unit
}

object Publisher {
  def apply(endpointId: String) = new Publisher(endpointId)
}

class Publisher(val endpointId: String) extends OpProvider[Pub] {
  def provide(ec: EvalContext): Pub = new Pub {
    def pub(uri: String): Unit = println(s"Publishing $uri to $endpointId")
  }

  override def onSuccess(task: Task[_], z: Any): Unit =
    println(s"${task.id} completed with $z")
}
