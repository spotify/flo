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
    .input(greet(person))
    .process { (greeting) =>
      s"$greeting, nice to meet you"
    }

  def meetDsl(person: String): Task[String] = defTask[String](person) dsl ($
    input      greet(person)
    process ( greeting =>
      s"$greeting, nice to meet you"
    )
  )

  def world(arg: Int): Task[String] = defTask[String](arg) dsl (
    $ input fib(arg)
      process  (a => s"world $arg:$a")
  )

  def world2(arg: Int): Task[String] = defTaskNamed("custom-name", arg) process {
    s"world $arg"
  }

  def hello: Task[String] = defTask[String]() dsl (
    $ input world(0)
      inputs List(world(7), world(14))
      input world2(21)
      inputs List(world2(22))
      process ((a, b, c, d) => s"hello $a $b $c $d")
  )

  def readableDsl: Task[String] = defTask[String]() dsl ($
    // upstream dependencies
    input hello
    input world(7)

    // we'll publish this endpoint when the task is done
    context Publisher("MyEndpoint")

    // run function
    process daFoo
  )

  def daFoo(world: String, wo: String, pub1: Pub): String = {
    pub1.pub("gs://foo/bar")
    "ok"
  }

  def hello2: Task[String] = defTask[String]() dsl ($
    input      world(0)
    inputs     List(world(7), world(14))
    input      world2(21)
    inputs     List(world2(22))
    process ((a, b, c, d) => s"hello $a $b $c $d")
  )

  def fib(n: Int): Task[Int] = defTask[Int](n) dsl (
    if (n < 2)
      $ process n
    else
      $ input fib(n - 1)
        input fib(n - 2)
        process (_ + _)
  )

  def fib2(n: Int): Task[Int] = defTask[Int](n) dsl (
    if (n < 2)
      $ process n
    else
      $ input fib2(n - 1)
        input fib2(n - 2)
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

class Publisher(val endpointId: String) extends TaskContextGeneric[Pub] {
  def provide(ec: EvalContext): Pub = new Pub {
    def pub(uri: String): Unit = println(s"Publishing $uri to $endpointId")
  }

  override def onSuccess(task: Task[_], z: Any): Unit =
    println(s"${task.id} completed with $z")
}
