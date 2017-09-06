package com.spotify.flo

import org.scalatest._

class DslTest extends FlatSpec with Matchers {

  private val metaKey = MetaKey.create[Int]("foo")

  "A defTask `$` builder" can "be accessed in dsl scope to create a task" in {
    defTask[String]() dsl ($ -> "hello") shouldBe a [Task[_]]
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

  it should "store metadata key-values" in {
    def task = defTaskNamed("with-meta")
      .meta(metaKey, 42)
      .process("hello")

    task.getMeta(metaKey) shouldBe 42
  }

  it should "store metadata key-values 2" in {
    def task = defTaskNamed("with-meta")
      .in(defTaskNamed("inner").process("bla"))
      .meta(metaKey, 42)
      .process(_ => "hello")

    task.getMeta(metaKey) shouldBe 42
  }

  it must "throw a RuntimeException if accessed outside of defTask" in {
    val exception = the [RuntimeException] thrownBy $

    exception should have message "Builder accessor used outside a defTask scope"
  }

  def classMethod: Task[String] = defTask().process("hello")
}
