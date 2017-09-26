package com.spotify.flo.dsl

import com.spotify.flo.TaskBuilder._
import com.spotify.flo._

import scala.collection.JavaConversions

/**
  * Helper functions for converting scala lambdas to flo Fn objects
  */
private[dsl] object Util {

  def fn[Z](fn: => Z) = new Fn[Z] {
    override def get(): Z = fn
  }

  def fn1[A, Z](fn: (A) => Z) = new Fn1[A, Z] {
    override def apply(a: A): Z = fn(a)
  }

  def f0[Z](fn: => Z) = new F0[Z] {
    override def get(): Z = fn
  }

  def f1[A, Z](fn: (A) => Z) = new F1[A, Z] {
    override def apply(a: A): Z = fn(a)
  }

  def f2[A, B, Z](fn: (A, B) => Z) = new F2[A, B, Z] {
    override def apply(a: A, b: B): Z = fn(a, b)
  }

  def f3[A, B, C, Z](fn: (A, B, C) => Z) = new F3[A, B, C, Z] {
    override def apply(a: A, b: B, c: C): Z = fn(a, b, c)
  }

  def f4[A, B, C, D, Z](fn: (A, B, C, D) => Z) = new F4[A, B, C, D, Z] {
    override def apply(a: A, b: B, c: C, d: D): Z = fn(a, b, c, d)
  }

  def f5[A, B, C, D, E, Z](fn: (A, B, C, D, E) => Z) = new F5[A, B, C, D, E, Z] {
    override def apply(a: A, b: B, c: C, d: D, e: E): Z = fn(a, b, c, d, e)
  }

  def f6[A, B, C, D, E, F, Z](fn: (A, B, C, D, E, F) => Z) = new F6[A, B, C, D, E, F, Z] {
    override def apply(a: A, b: B, c: C, d: D, e: E, f: F): Z = fn(a, b, c, d, e, f)
  }

  def f7[A, B, C, D, E, F, G, Z](fn: (A, B, C, D, E, F, G) => Z) = new F7[A, B, C, D, E, F, G, Z] {
    override def apply(a: A, b: B, c: C, d: D, e: E, f: F, g: G): Z = fn(a, b, c, d, e, f, g)
  }

  def f8[A, B, C, D, E, F, G, H, Z](fn: (A, B, C, D, E, F, G, H) => Z) = new F8[A, B, C, D, E, F, G, H, Z] {
    override def apply(a: A, b: B, c: C, d: D, e: E, f: F, g: G, h: H): Z = fn(a, b, c, d, e, f, g, h)
  }

  def f9[A, B, C, D, E, F, G, H, I, Z](fn: (A, B, C, D, E, F, G, H, I) => Z) = new F9[A, B, C, D, E, F, G, H, I, Z] {
    override def apply(a: A, b: B, c: C, d: D, e: E, f: F, g: G, h: H, i: I): Z = fn(a, b, c, d, e, f, g, h, i)
  }

  def f10[A, B, C, D, E, F, G, H, I, J, Z](fn: (A, B, C, D, E, F, G, H, I, J) => Z) = new F10[A, B, C, D, E, F, G, H, I, J, Z] {
    override def apply(a: A, b: B, c: C, d: D, e: E, f: F, g: G, h: H, i: I, j: J): Z = fn(a, b, c, d, e, f, g, h, i, j)
  }

  def f11[A, B, C, D, E, F, G, H, I, J, K, Z](fn: (A, B, C, D, E, F, G, H, I, J, K) => Z) = new F11[A, B, C, D, E, F, G, H, I, J, K, Z] {
    override def apply(a: A, b: B, c: C, d: D, e: E, f: F, g: G, h: H, i: I, j: J, k: K): Z = fn(a, b, c, d, e, f, g, h, i, j, k)
  }

  def f12[A, B, C, D, E, F, G, H, I, J, K, L, Z](fn: (A, B, C, D, E, F, G, H, I, J, K, L) => Z) = new F12[A, B, C, D, E, F, G, H, I, J, K, L, Z] {
    override def apply(a: A, b: B, c: C, d: D, e: E, f: F, g: G, h: H, i: I, j: J, k: K, l: L): Z = fn(a, b, c, d, e, f, g, h, i, j, k, l)
  }

  def f13[A, B, C, D, E, F, G, H, I, J, K, L, M, Z](fn: (A, B, C, D, E, F, G, H, I, J, K, L, M) => Z) = new F13[A, B, C, D, E, F, G, H, I, J, K, L, M, Z] {
    override def apply(a: A, b: B, c: C, d: D, e: E, f: F, g: G, h: H, i: I, j: J, k: K, l: L, m: M): Z = fn(a, b, c, d, e, f, g, h, i, j, k, l, m)
  }

  def f14[A, B, C, D, E, F, G, H, I, J, K, L, M, N, Z](fn: (A, B, C, D, E, F, G, H, I, J, K, L, M, N) => Z) = new F14[A, B, C, D, E, F, G, H, I, J, K, L, M, N, Z] {
    override def apply(a: A, b: B, c: C, d: D, e: E, f: F, g: G, h: H, i: I, j: J, k: K, l: L, m: M, n: N): Z = fn(a, b, c, d, e, f, g, h, i, j, k, l, m, n)
  }

  def f15[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, Z](fn: (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O) => Z) = new F15[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, Z] {
    override def apply(a: A, b: B, c: C, d: D, e: E, f: F, g: G, h: H, i: I, j: J, k: K, l: L, m: M, n: N, o: O): Z = fn(a, b, c, d, e, f, g, h, i, j, k, l, m, n, o)
  }

  def f16[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Z](fn: (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P) => Z) = new F16[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Z] {
    override def apply(a: A, b: B, c: C, d: D, e: E, f: F, g: G, h: H, i: I, j: J, k: K, l: L, m: M, n: N, o: O, p: P): Z = fn(a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p)
  }

  def f17[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, Z](fn: (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q) => Z) = new F17[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, Z] {
    override def apply(a: A, b: B, c: C, d: D, e: E, f: F, g: G, h: H, i: I, j: J, k: K, l: L, m: M, n: N, o: O, p: P, q: Q): Z = fn(a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q)
  }

  def f18[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, Z](fn: (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R) => Z) = new F18[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, Z] {
    override def apply(a: A, b: B, c: C, d: D, e: E, f: F, g: G, h: H, i: I, j: J, k: K, l: L, m: M, n: N, o: O, p: P, q: Q, r: R): Z = fn(a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r)
  }

  def f19[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, Z](fn: (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S) => Z) = new F19[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, Z] {
    override def apply(a: A, b: B, c: C, d: D, e: E, f: F, g: G, h: H, i: I, j: J, k: K, l: L, m: M, n: N, o: O, p: P, q: Q, r: R, s: S): Z = fn(a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s)
  }

  def javaList[A](in: => List[Task[A]]): Fn[java.util.List[Task[A]]] =
    Util.fn(JavaConversions.seqAsJavaList(in))
}
