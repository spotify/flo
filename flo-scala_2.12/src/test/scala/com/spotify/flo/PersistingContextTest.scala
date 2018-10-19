package com.spotify.flo

import java.nio.file.Files

import org.scalatest._

class PersistingContextTest extends FlatSpec with Matchers {

  "PersistingContext" should "be able to serialize and deserialize scala collections" in {
    val f = Files.createTempFile(null, null)
    Files.deleteIfExists(f)
    try {
      val v = (List(Some("foo"), None, Some(4711)), None, Some("baz"))
      Serialization.serialize(v, f)
      val deserialized = Serialization.deserialize(f).asInstanceOf[Any]
      deserialized shouldBe v
    } finally {
      Files.deleteIfExists(f)
    }
  }
}
