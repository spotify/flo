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

  def task[T: ClassTag](name: String, args: Any*): TaskBuilder0[T] =
    named[T](name, args: _*)

  def defTask[T: ClassTag](name: String, args: Any*)(f: => Task[T]): Task[T] =
    dynamicBuilder.withValue(named[T](name, args: _*))(f)

  def defTask[T: ClassTag](args: Any*)(f: => Task[T]): Task[T] =
    dynamicBuilder.withValue(named[T](currentMethodName(), args: _*))(f)

  def defTask[T: ClassTag](f: => Task[T]): Task[T] =
    dynamicBuilder.withValue(named[T](currentMethodName()))(f)

  // requires parens on call
  def ->[T](code: => T): Task[T] = currentBuilder.process(code)
  def ┌[A, T](task: => Task[A]): TaskBuilder1[A, T] = currentBuilder.in(task)

  def ▫  [T]: TaskBuilder0[T] = currentBuilder
  def ▫─┐[T]: TaskBuilder0[T] = currentBuilder
  def └─┐[T]: TaskBuilder0[T] = currentBuilder
  def ─  [T]: TaskBuilder0[T] = currentBuilder
  def ┬  [T]: TaskBuilder0[T] = currentBuilder
  def ├  [T]: TaskBuilder0[T] = currentBuilder
  def └  [T]: TaskBuilder0[T] = currentBuilder
  def >  [T]: TaskBuilder0[T] = currentBuilder
  def $  [T]: TaskBuilder0[T] = currentBuilder

  def T  [T]: TaskBuilder0[T] = currentBuilder
  def F  [T]: TaskBuilder0[T] = currentBuilder

  def branch[Z](condition: Boolean)(t: => Task[Z])(f: => Task[Z]): Task[Z] =
    if (condition) t else f

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
