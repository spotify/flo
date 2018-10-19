/*-
 * -\-\-
 * Flo Freezer
 * --
 * Copyright (C) 2016 - 2018 Spotify AB
 * --
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * -/-/-
 */

package com.spotify.flo.freezer;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;

import com.spotify.flo.Fn;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.NotSerializableException;
import java.io.PrintWriter;
import java.io.Serializable;
import java.io.StreamCorruptedException;
import java.io.StringWriter;
import java.lang.invoke.SerializedLambda;
import java.lang.reflect.Method;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

public class PersistingContextTest {

  @Rule public ExpectedException exception = ExpectedException.none();
  @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void exceptionSerialization() throws Exception {
    final RuntimeException bar = new RuntimeException("bar");
    bar.addSuppressed(new IOException("baz", new InterruptedException("quux")));
    final Exception exception1 = new Exception("foo", bar);
    exception1.addSuppressed(new FoobarException());
    final Exception exception = exception1;

    final Path exceptionFile = temporaryFolder.getRoot().toPath().resolve("exception");
    PersistingContext.serialize(exception, exceptionFile);

    final Exception deserialized = PersistingContext.deserialize(exceptionFile);

    // Verify that stack trace can be accessed and printed and seems to be correct

    deserialized.printStackTrace();

    final StringWriter sw = new StringWriter();
    final PrintWriter pw = new PrintWriter(sw);
    deserialized.printStackTrace(pw);
    pw.flush();
    final String stacktrace = sw.toString();
    assertThat(stacktrace, containsString("java.lang.Exception: foo"));
    assertThat(stacktrace, containsString("at com.spotify.flo.freezer.PersistingContextTest.exceptionSerialization"));
    assertThat(stacktrace, containsString("Suppressed: com.spotify.flo.freezer.PersistingContextTest$FoobarException"));
    assertThat(stacktrace, containsString("Caused by: java.lang.RuntimeException: bar"));
    assertThat(stacktrace, containsString("Suppressed: java.io.IOException: baz"));
    assertThat(stacktrace, containsString("Caused by: java.lang.InterruptedException: quux"));

    assertThat(deserialized.getStackTrace().length, is(not(0)));
    assertThat(deserialized.getStackTrace()[0].getClassName(), is("com.spotify.flo.freezer.PersistingContextTest"));
    assertThat(deserialized.getStackTrace()[0].getMethodName(), is("exceptionSerialization"));
  }

  @Test
  public void serializeShouldPropagateSerializationExceptions() {
    exception.expect(RuntimeException.class);
    exception.expectCause(instanceOf(NotSerializableException.class));
    PersistingContext.serialize(new Object(), new ByteArrayOutputStream());
  }

  @Test
  public void serializeShouldPropagateIOException() throws Exception {
    exception.expect(RuntimeException.class);
    exception.expectCause(instanceOf(NoSuchFileException.class));
    PersistingContext.serialize("foobar", Paths.get("non-existent-dir", "file"));
  }

  @Test
  public void deserializeShouldPropagateSerializationExceptions() {
    exception.expect(RuntimeException.class);
    exception.expectCause(instanceOf(StreamCorruptedException.class));
    PersistingContext.deserialize(new ByteArrayInputStream("foobar".getBytes()));
  }

  @Test
  public void shouldProvideDetailedDebugInformationOnSerializationExceptions() throws ReflectiveOperationException {
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

    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    final Fn<Foo> fn = () -> foo;
    try {
      PersistingContext.serialize(fn, baos);
    } catch (Exception e) {
      assertThat(e.getMessage(), is(
          "java.io.NotSerializableException: com.spotify.flo.freezer.PersistingContextTest$1Quux\n"
              + "\t- field (class \"com.spotify.flo.freezer.PersistingContextTest$1Baz\", name: \"quux\", type: \"class com.spotify.flo.freezer.PersistingContextTest$1Quux\")\n"
              + "\t- object (class \"com.spotify.flo.freezer.PersistingContextTest$1Baz\", com.spotify.flo.freezer.PersistingContextTest$1Baz@" + Integer.toHexString(System.identityHashCode(foo.bar.baz)) + ")\n"
              + "\t- field (class \"com.spotify.flo.freezer.PersistingContextTest$1Bar\", name: \"baz\", type: \"class com.spotify.flo.freezer.PersistingContextTest$1Baz\")\n"
              + "\t- object (class \"com.spotify.flo.freezer.PersistingContextTest$1Bar\", com.spotify.flo.freezer.PersistingContextTest$1Bar@" + Integer.toHexString(System.identityHashCode(foo.bar)) + ")\n"
              + "\t- field (class \"com.spotify.flo.freezer.PersistingContextTest$1Foo\", name: \"bar\", type: \"class com.spotify.flo.freezer.PersistingContextTest$1Bar\")\n"
              + "\t- object (class \"com.spotify.flo.freezer.PersistingContextTest$1Foo\", com.spotify.flo.freezer.PersistingContextTest$1Foo@" + Integer.toHexString(System.identityHashCode(foo)) + ")\n"
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

  private static class FoobarException extends Exception {

  }
}