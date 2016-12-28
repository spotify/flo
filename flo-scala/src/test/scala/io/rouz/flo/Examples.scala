package io.rouz.flo

import java.util.function.Consumer

/**
  * Example task definitions that should compile
  */
object Examples {

  def world(arg: Int): Task[String] = defTask(arg)(
    $ ┬ fib(arg)
      └> (a => s"world $arg:$a")
  )

  def world2(arg: Int): Task[String] = defTask("custom-name", arg)(
    $ -> s"world $arg"
  )

  def hello: Task[String] = defTask(
    $ ┬ world(0)
      ╞ List(world(7), world(14))
      ├ world2(21)
      ╞ List(world2(22))
      └> ((a, b, c, d) => s"hello $a $b $c $d")
  )

  def hello2: Task[String] = defTask(>
    in      world(0)
    ins     List(world(7), world(14))
    in      world2(21)
    ins     List(world2(22))
    process ((a, b, c, d) => s"hello $a $b $c $d")
  )

  def fib(n: Int): Task[Int] = defTask(n)(
    if (n < 2)
      $ -> n
    else
      $ ┬ fib(n - 1)
        ├ fib(n - 2)
        └> (_ + _)
  )


  def main(args: Array[String]): Unit = {
    print(TaskInfo.ofTask(hello))

    TaskContext.inmem.evaluate(hello)
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
