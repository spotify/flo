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
    val foo = defTaskNamed("foo")
      .process(() => {
        val jvm = ManagementFactory.getRuntimeMXBean.getName
        println("foo jvm: " + jvm)
        jvm
      })
    val bar = defTaskNamed("bar")
      .input(foo)
      .context(new TaskContextStrict[String, Array[String]] {
        override def provide(evalContext: EvalContext): String = "hello"
      })
      .process((fooName, _) => {
        val jvm = ManagementFactory.getRuntimeMXBean.getName
        println("bar jvm: " + jvm)
        Array(fooName(), jvm)
      })
    val result: Array[String] = FloRunner.runTask(bar)
      .future().get()
    result.toSet should have size 2
  }

  def classMethod: Task[String] = defTask().process("hello")
}
