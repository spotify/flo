package com.spotify.flo

import java.nio.file.Files

import com.esotericsoftware.kryo.Kryo
import com.spotify.flo.freezer.PersistingContext
import org.scalatest._

class PersistingContextTest extends FlatSpec with Matchers {

  "com.twitter.chill.AllScalaRegistrar" should "be available" in {
    val cls = Class.forName("com.twitter.chill.AllScalaRegistrar")
    val kryo = new Kryo
    val registrar = cls.newInstance().asInstanceOf[com.twitter.chill.AllScalaRegistrar]
    registrar.apply(kryo)
  }

  "PersistingContext" should "be able to serialize and deserialize scala collections" in {
    val f = Files.createTempFile(null, null)
    Files.deleteIfExists(f)
    try {
      val v = (List(Some("foo"), None, Some(4711)), None, Some("baz"))
      PersistingContext.serialize(v, f)
      val deserialized = PersistingContext.deserialize(f).asInstanceOf[Any]
      deserialized shouldBe v
    } finally {
      Files.deleteIfExists(f)
    }
  }
}
