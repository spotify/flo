package com.spotify.flo;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.*;

import java.io.ByteArrayOutputStream;
import java.io.NotSerializableException;
import java.io.Serializable;
import java.lang.invoke.SerializedLambda;
import java.lang.reflect.Method;
import org.junit.Test;

public class BuilderUtilsTest {

  @Test
  public void shouldProvideDetailedDebugInformationOnNotSerializableException() throws ReflectiveOperationException {
    class Quux {}
    class Baz implements Serializable {
      final Quux quux = new Quux();
    }
    class Bar implements Serializable {
      final Baz baz = new Baz();
    }
    class Foo implements Serializable {
      final Bar bar = new Bar();
    }
    final Foo foo = new Foo();

    final Fn<Foo> fn = () -> foo;
    try {
      BuilderUtils.requireSerializable(fn, "process fn");
    } catch (IllegalArgumentException e) {
      final Throwable cause = e.getCause();
      assertThat(cause, instanceOf(NotSerializableException.class));
      assertThat(cause.toString(), is(
          "java.io.NotSerializableException: com.spotify.flo.BuilderUtilsTest$1Quux\n"
              + "\t- field (class \"com.spotify.flo.BuilderUtilsTest$1Baz\", name: \"quux\", type: \"class com.spotify.flo.BuilderUtilsTest$1Quux\")\n"
              + "\t- object (class \"com.spotify.flo.BuilderUtilsTest$1Baz\", com.spotify.flo.BuilderUtilsTest$1Baz@" + Integer.toHexString(System.identityHashCode(foo.bar.baz)) + ")\n"
              + "\t- field (class \"com.spotify.flo.BuilderUtilsTest$1Bar\", name: \"baz\", type: \"class com.spotify.flo.BuilderUtilsTest$1Baz\")\n"
              + "\t- object (class \"com.spotify.flo.BuilderUtilsTest$1Bar\", com.spotify.flo.BuilderUtilsTest$1Bar@" + Integer.toHexString(System.identityHashCode(foo.bar)) + ")\n"
              + "\t- field (class \"com.spotify.flo.BuilderUtilsTest$1Foo\", name: \"bar\", type: \"class com.spotify.flo.BuilderUtilsTest$1Bar\")\n"
              + "\t- object (class \"com.spotify.flo.BuilderUtilsTest$1Foo\", com.spotify.flo.BuilderUtilsTest$1Foo@" + Integer.toHexString(System.identityHashCode(foo)) + ")\n"
              + "\t- element of array (index: 0)\n"
              + "\t- array (class \"[Ljava.lang.Object;\", size: 1)\n"
              + "\t- field (class \"java.lang.invoke.SerializedLambda\", name: \"capturedArgs\", type: \"class [Ljava.lang.Object;\")\n"
              + "\t- root object (class \"java.lang.invoke.SerializedLambda\", " + serializedLambda(fn) + ")"));
    }
  }

  private static SerializedLambda serializedLambda(Object o) throws ReflectiveOperationException {
    final Method m = o.getClass().getDeclaredMethod("writeReplace");
    m.setAccessible(true);
    return (SerializedLambda) m.invoke(o);
  }
}