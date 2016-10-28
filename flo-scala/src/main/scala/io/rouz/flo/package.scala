package io.rouz

import io.rouz.flo.scala.FloTask.named

import scala.language.dynamics
import scala.reflect.ClassTag

/**
  * Package object for creating flo tasks
  */
package object flo extends Dynamic {

  def selectDynamic[T: ClassTag](method: String) = task[T](method)
  def applyDynamic[T: ClassTag](method: String)(args: Any*) = task[T](method, args:_*)

  def task[T: ClassTag](name: String, args: Any*) = named[T](name, args:_*)
}
