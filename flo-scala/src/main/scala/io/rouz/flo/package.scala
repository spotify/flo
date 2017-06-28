package io.rouz

import java.lang.Thread.currentThread

import io.rouz.flo.dsl.FloTask.named
import io.rouz.flo.dsl.TaskBuilder0
import io.rouz.flo.dsl.TaskBuilder1

import scala.language.dynamics
import scala.language.implicitConversions
import scala.reflect.ClassTag
import scala.util.DynamicVariable

/**
  * Package object with task wiring dsl helpers
  */
package object flo {

  def defTask[T: ClassTag](args: Any*): TaskBuilder0[T] =
    named[T](currentMethodName(), args: _*)

  def defTaskNamed[T: ClassTag](name: String, args: Any*): TaskBuilder0[T] =
    named[T](name, args: _*)

  implicit def toTask[T](t: => T): Task[T] = currentBuilder.process(t)

  implicit class TB0Dsl[T](val tb0: TaskBuilder0[T]) extends AnyVal {
    def dsl(f: => Task[T]): Task[T] =
      dynamicBuilder.withValue(tb0)(f)
  }

  def $[T]: TaskBuilder0[T] = currentBuilder
  def ▫[T]: TaskBuilder0[T] = currentBuilder

  private def currentBuilder[T]: TaskBuilder0[T] = {
    val builder = dynamicBuilder.value
    if (builder == null) {
      throw new RuntimeException("Builder accessor used outside a defTask scope")
    }

    builder.asInstanceOf[TaskBuilder0[T]]
  }
  private val dynamicBuilder = new DynamicVariable[TaskBuilder0[_]](null)
  private def currentMethodName(): String =
    currentThread.getStackTrace()(3).getMethodName.replaceAll("\\$.*$", "")
}
