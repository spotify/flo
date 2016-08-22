package io.rouz.task.scala

import io.rouz.task._
import io.rouz.task.TaskContext.Value
import TaskBuilder._

import _root_.scala.collection.JavaConversions
import _root_.scala.reflect.ClassTag

object FloTask {
  def named[Z: ClassTag](name: String, args: AnyRef*): TaskBuilder0[Z] = new TB(name, args:_*)
}

trait TaskBuilder0[Z] {
  def process(code: => Z): Task[Z]
  def processWithContext(code: TaskContext => Value[Z]): Task[Z]
  def in[A](task: => Task[A]): TaskBuilder1[A, Z]
  def ins[A](tasks: => List[Task[A]]): TaskBuilder1[List[A], Z]
}

trait TaskBuilder1[A, Z] {
  def process(code: A => Z): Task[Z]
  def processWithContext(code: TaskContext => A => Value[Z]): Task[Z]
  def in[B](task: => Task[B]): TaskBuilder2[A, B, Z]
  def ins[B](tasks: => List[Task[B]]): TaskBuilder2[A, List[B], Z]
}

trait TaskBuilder2[A, B, Z] {
  def process(code: (A,B) => Z): Task[Z]
  def processWithContext(code: TaskContext => (A,B) => Value[Z]): Task[Z]
  def in[C](task: => Task[C]): TaskBuilder3[A, B, C, Z]
  def ins[C](tasks: => List[Task[C]]): TaskBuilder3[A, B, List[C], Z]
}

trait TaskBuilder3[A, B, C, Z] {
  def process(code: (A,B,C) => Z): Task[Z]
  def processWithContext(code: TaskContext => (A,B,C) => Value[Z]): Task[Z]
}

class TB[Z: ClassTag](val name: String, val args: AnyRef*) extends TaskBuilder0[Z] { self =>

  import Util._

  private val cls = implicitly[ClassTag[Z]].runtimeClass.asInstanceOf[Class[Z]]
  private val builder = Task
      .named(name, args:_*)
      .ofType(cls)

  override def process(code: => Z): Task[Z] =
    builder.process(f0(code))

  override def processWithContext(code: (TaskContext) => Value[Z]): Task[Z] =
    builder.processWithContext(f1(code))

  override def in[A](task: => Task[A]): TaskBuilder1[A, Z] = new Builder1[A, Z] {
    type JA = A
    val convA: JA => A = identity
    val builder = self.builder.in(fn[Task[A]](task))
  }

  override def ins[A](tasks: => List[Task[A]]) = new Builder1[List[A], Z] {
    type JA = java.util.List[A]
    val convA: JA => List[A] = a => JavaConversions.iterableAsScalaIterable(a).toList
    val builder = self.builder.ins(javaList(tasks))
  }
}

trait Builder1[A, Z] extends TaskBuilder1[A, Z] { self =>

  import Util._

  type JA
  val convA: JA => A
  val builder: TaskBuilder.TaskBuilder1[JA, Z]

  override def process(code: (A) => Z): Task[Z] =
    builder.process(f1(a => code(convA(a))))

  override def processWithContext(code: (TaskContext) => (A) => Value[Z]): Task[Z] =
    builder.processWithContext(f2((tc, a) => code(tc)(convA(a))))

  override def in[B](task: => Task[B]): TaskBuilder2[A, B, Z] = new Builder2[A, B, Z] {
    type JA = self.JA
    type JB = B
    val convA = self.convA
    val convB: JB => B = identity
    val builder = self.builder.in(fn[Task[B]](task))
  }

  override def ins[B](tasks: => List[Task[B]]): TaskBuilder2[A, List[B], Z] =
    new Builder2[A, List[B], Z] {
      type JA = self.JA
      type JB = java.util.List[B]
      val convA = self.convA
      val convB: JB => List[B] = b => JavaConversions.iterableAsScalaIterable(b).toList
      val builder = self.builder.ins(javaList(tasks))
    }
}

trait Builder2[A, B, Z] extends TaskBuilder2[A, B, Z] { self =>

  import Util._

  type JA
  type JB
  val convA: JA => A
  val convB: JB => B
  val builder: TaskBuilder.TaskBuilder2[JA, JB, Z]

  override def process(code: (A, B) => Z): Task[Z] =
    builder.process(f2((a,b) => code(convA(a), convB(b))))

  override def processWithContext(code: (TaskContext) => (A, B) => Value[Z]): Task[Z] =
    builder.processWithContext(f3((tc,a,b) => code(tc)(convA(a), convB(b))))

  override def in[C](task: => Task[C]): TaskBuilder3[A, B, C, Z] = new Builder3[A, B, C, Z] {
    type JA = self.JA
    type JB = self.JB
    type JC = C
    val convA = self.convA
    val convB = self.convB
    val convC: JC => C = identity
    val builder = self.builder.in(fn[Task[C]](task))
  }

  override def ins[C](tasks: => List[Task[C]]): TaskBuilder3[A, B, List[C], Z] = new Builder3[A, B, List[C], Z] {
    type JA = self.JA
    type JB = self.JB
    type JC = java.util.List[C]
    val convA = self.convA
    val convB = self.convB
    val convC: JC => List[C] = c => JavaConversions.iterableAsScalaIterable(c).toList
    val builder = self.builder.ins(javaList(tasks))
  }
}

trait Builder3[A, B, C, Z] extends TaskBuilder3[A, B, C, Z] {

  import Util._

  type JA
  type JB
  type JC
  val convA: JA => A
  val convB: JB => B
  val convC: JC => C
  val builder: TaskBuilder.TaskBuilder3[JA, JB, JC, Z]

  override def process(code: (A, B, C) => Z): Task[Z] =
    builder.process(f3((a,b,c) => code(convA(a), convB(b), convC(c))))

  override def processWithContext(code: (TaskContext) => (A, B, C) => Value[Z]): Task[Z] =
    builder.processWithContext(f4((tc,a,b,c) => code(tc)(convA(a), convB(b), convC(c))))
}

private object Util {
  def lazyList(lazyTasks: (() => Task[_])*): () => List[Task[_]] =
    () => lazyTasks.map(_()).toList

  def lazyFlatten[T](lazyLists: (() => List[T])*): () => List[T] =
    () => lazyLists.map(_()).flatMap(identity).toList

  def fn[A](fn: => A): Fn[A] = new Fn[A] {
    override def get(): A = fn
  }

  def f0[A](fn: => A): F0[A] = new F0[A] {
    override def get(): A = fn
  }

  def f1[A, B](fn: A => B): F1[A, B] = new F1[A, B] {
    override def apply(a: A): B = fn(a)
  }

  def f2[A, B, C](fn: (A,B) => C): F2[A, B, C] = new F2[A, B, C] {
    override def apply(a: A, b: B): C = fn(a, b)
  }

  def f3[A, B, C, D](fn: (A,B,C) => D): F3[A, B, C, D] = new F3[A, B, C, D] {
    override def apply(a: A, b: B, c: C): D = fn(a, b, c)
  }

  def f4[A, B, C, D, E](fn: (A,B,C,D) => E): F4[A, B, C, D, E] = new F4[A, B, C, D, E] {
    override def apply(a: A, b: B, c: C, d: D): E = fn(a, b, c, d)
  }

  def fn1[A, B](fn: A => B): java.util.function.Function[A, B] = new java.util.function.Function[A, B] {
    override def apply(a: A): B = fn(a)
  }

  def fn0[A](fn: A => Unit): java.util.function.Consumer[A] = new java.util.function.Consumer[A] {
    override def accept(a: A): Unit = fn(a)
  }

  def javaList[A](in: => List[Task[A]]): Fn[java.util.List[Task[A]]] =
    Util.fn(JavaConversions.seqAsJavaList(in))
}
