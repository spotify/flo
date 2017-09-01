package com.spotify

import java.lang.Thread.currentThread

import com.spotify.flo.dsl.FloTask.named
import com.spotify.flo.dsl.TaskBuilder0

import scala.language.implicitConversions
import scala.reflect.ClassTag
import scala.util.DynamicVariable

/**
  * Package object with task wiring dsl helpers
  */
package object flo {

  def defTask[T: ClassTag](args: Any*): TaskBuilder0[T] =
    named[T](currentMethodName(), args: _*)

  trait Comment
  case object ╎ extends Comment

  def defTaskNamed[T: ClassTag](name: String, args: Any*): TaskBuilder0[T] =
    named[T](name, args: _*)

  implicit class TB0Dsl[T](val tb0: TaskBuilder0[T]) extends AnyVal {
    def dsl(f: => Task[T]): Task[T] =
      dynamicBuilder.withValue(tb0)(f)
  }

  def $[T]: TaskBuilder0[T] = currentBuilder
  def ▫[T]: TaskBuilder0[T] = currentBuilder
  def ▫─╮[T]: TaskBuilder0[T] = currentBuilder

  //  def ▫  [T]: TaskBuilder0[T] = currentBuilder
  //  def ▫─┐[T]: TaskBuilder0[T] = currentBuilder
  //  def └─┐[T]: TaskBuilder0[T] = currentBuilder
  //  def ╰─╮[T]: TaskBuilder0[T] = currentBuilder
  //  def ─  [T]: TaskBuilder0[T] = currentBuilder
  //  def ┬  [T]: TaskBuilder0[T] = currentBuilder
  //  def ├  [T]: TaskBuilder0[T] = currentBuilder
  //  def └  [T]: TaskBuilder0[T] = currentBuilder
  //  def >  [T]: TaskBuilder0[T] = currentBuilder
  //  def $  [T]: TaskBuilder0[T] = currentBuilder

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
