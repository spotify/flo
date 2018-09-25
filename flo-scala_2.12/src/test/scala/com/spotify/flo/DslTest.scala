package com.spotify.flo

import java.lang.management.ManagementFactory

import com.spotify.flo.context.FloRunner
import org.scalatest._

class DslTest extends FlatSpec with Matchers {

  "A defTask `$` builder" can "be accessed in dsl scope to create a task" in {
    defTask[String]() dsl ($ process "hello") shouldBe a [Task[_]]
  }

  it should "create a task using the name of the enclosing class method" in {
    classMethod.id.name shouldBe "classMethod"
  }

  it should "create a task using the name of the enclosing inner method" in {
    def innerMethod = defTask().process("hello")

    innerMethod.id.name shouldBe "innerMethod"
  }

  it should "create a task with a specific name" in {
    def task = defTaskNamed("specific-name").process("hello")

    task.id.name shouldBe "specific-name"
  }

  it should "create a task with arguments" in {
    def task = defTask(1, 2, 3).process("hello")

    task.id.toString shouldBe "task(1,2,3)#2ac733ae"
  }

  it must "throw a RuntimeException if accessed outside of defTask" in {
    val exception = the [RuntimeException] thrownBy $

    exception should have message "Builder accessor used outside a defTask scope"
  }

  it should "runInProcesses" in {
    val mainJvm = ManagementFactory.getRuntimeMXBean.getName
    println(s"main jvm: $mainJvm")
    val foo: Task[String] = defTaskNamed("foo")
      .process({
        val fooJvm = ManagementFactory.getRuntimeMXBean.getName
        println(s"foo jvm: $fooJvm")
        fooJvm
      })
    val bar: Task[Array[String]] = defTaskNamed("bar")
      .input(foo)
      .output(new TaskOutput[String, Array[String]] {
        override def provide(evalContext: EvalContext): String = {
          val barContextJvm = ManagementFactory.getRuntimeMXBean.getName
          println(s"bar context jvm: $barContextJvm")
          barContextJvm
        }
      })
      .process((fooJvm, barContextJvm) => {
        val barJvm = ManagementFactory.getRuntimeMXBean.getName
        println(s"bar jvm: $barJvm")
        Array(fooJvm, barJvm, barContextJvm)
      })

    val result = FloRunner.runTask(bar)
      .future().get()

    val Array(fooJvm, barJvm, barContextJvm) = result
    result.toSet should have size 3
    fooJvm should not be (mainJvm)
    barJvm should not be (mainJvm)
    barContextJvm should be (mainJvm)
  }

  it should "be possible to use scala collections" in {

    val captured = List(None, Some("baz"))

    val foo: Task[(List[Option[Any]], List[Option[Any]])] = defTaskNamed("foo")
      .process({
        (List(Some("foo"), None, Some(4711)), captured)
      })

    val bar: Task[(List[Option[Any]], List[Option[Any]])] = defTaskNamed("bar")
      .input(foo)
      .process(fooTuple => {
        fooTuple
      })

    val result = FloRunner.runTask(bar)
      .future().get()

    result shouldBe (List(Some("foo"), None, Some(4711)), captured)
  }

  def classMethod: Task[String] = defTask().process("hello")
}
