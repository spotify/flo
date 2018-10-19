/*-
 * -\-\-
 * Flo Workflow Definition
 * --
 * Copyright (C) 2016 - 2017 Spotify AB
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

package com.spotify.flo;

import static com.spotify.flo.Serialization.deserialize;
import static com.spotify.flo.Serialization.serialize;
import static java.util.Collections.singletonList;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import com.spotify.flo.TaskBuilder.F0;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.NotSerializableException;
import java.io.ObjectStreamException;
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

public class SerializationTest {

  @Rule public ExpectedException exception = ExpectedException.none();
  @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();

  final String instanceField = "from instance";
  final EvalContext context = EvalContext.sync();
  final AwaitValue<String> val = new AwaitValue<>();

  @Test
  public void shouldJavaUtilSerialize() throws Exception {
    Task<Long> task1 = Task.named("Foo", "Bar", 39).ofType(Long.class)
        .process(() -> 9999L);
    Task<String> task2 = Task.named("Baz", 40).ofType(String.class)
        .input(() -> task1)
        .inputs(() -> singletonList(task1))
        .process((t1, t1l) -> t1l + " hello " + (t1 + 5));

    final byte[] serialized = serialize(task2);
    Task<String> des = deserialize(serialized);
    context.evaluate(des).consume(val);

    assertEquals(des.id().name(), "Baz");
    assertEquals(val.awaitAndGet(), "[9999] hello 10004");
  }

  @Test
  public void shouldNotSerializeWithInstanceFieldReference() throws Exception {
    final TaskBuilder<String> builder = Task.named("WithRef").ofType(String.class);
    final F0<String> fn = () -> instanceField + " causes an outer reference";

    exception.expect(IllegalArgumentException.class);
    exception.expectCause(instanceOf(NotSerializableException.class));
    builder.process(fn);
  }

  @Test
  public void shouldSerializeWithLocalReference() throws Exception {
    String local = instanceField;

    Task<String> task = Task.named("WithLocalRef").ofType(String.class)
        .process(() -> local + " won't cause an outer reference");

    final byte[] serialized = serialize(task);
    Task<String> des = deserialize(serialized);
    context.evaluate(des).consume(val);

    assertEquals(val.awaitAndGet(), "from instance won't cause an outer reference");
  }

  @Test
  public void shouldSerializeWithMethodArgument() throws Exception {
    Task<String> task = closure(instanceField);

    final byte[] serialized = serialize(task);
    Task<String> des = deserialize(serialized);
    context.evaluate(des).consume(val);

    assertEquals(val.awaitAndGet(), "from instance is enclosed");
  }

  private static Task<String> closure(String arg) {
    return Task.named("Closed").ofType(String.class).process(() -> arg + " is enclosed");
  }

  @Test
  public void shouldNotSerializeAnonymousClass() throws Exception {
    final TaskBuilder<String> builder = Task.named("WithAnonClass").ofType(String.class);
    final F0<String> fn = new F0<String>() {
      @Override
      public String get() {
        return "yes? no!";
      }
    };

    exception.expect(IllegalArgumentException.class);
    exception.expectCause(instanceOf(NotSerializableException.class));
    builder.process(fn);
  }

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
      Serialization.serialize(fn);
    } catch (ObjectStreamException e) {
      assertThat(e, instanceOf(NotSerializableException.class));
      assertThat(e.toString(), is(
          "java.io.NotSerializableException: com.spotify.flo.SerializationTest$1Quux\n"
              + "\t- field (class \"com.spotify.flo.SerializationTest$1Baz\", name: \"quux\", type: \"class com.spotify.flo.SerializationTest$1Quux\")\n"
              + "\t- object (class \"com.spotify.flo.SerializationTest$1Baz\", com.spotify.flo.SerializationTest$1Baz@" + Integer.toHexString(System.identityHashCode(foo.bar.baz)) + ")\n"
              + "\t- field (class \"com.spotify.flo.SerializationTest$1Bar\", name: \"baz\", type: \"class com.spotify.flo.SerializationTest$1Baz\")\n"
              + "\t- object (class \"com.spotify.flo.SerializationTest$1Bar\", com.spotify.flo.SerializationTest$1Bar@" + Integer.toHexString(System.identityHashCode(foo.bar)) + ")\n"
              + "\t- field (class \"com.spotify.flo.SerializationTest$1Foo\", name: \"bar\", type: \"class com.spotify.flo.SerializationTest$1Bar\")\n"
              + "\t- object (class \"com.spotify.flo.SerializationTest$1Foo\", com.spotify.flo.SerializationTest$1Foo@" + Integer.toHexString(System.identityHashCode(foo)) + ")\n"
              + "\t- element of array (index: 0)\n"
              + "\t- array (class \"[Ljava.lang.Object;\", size: 1)\n"
              + "\t- field (class \"java.lang.invoke.SerializedLambda\", name: \"capturedArgs\", type: \"class [Ljava.lang.Object;\")\n"
              + "\t- root object (class \"java.lang.invoke.SerializedLambda\", " + serializedLambda(fn) + ")"));
    }
  }

  @Test
  public void exceptionSerialization() throws Exception {
    final RuntimeException bar = new RuntimeException("bar");
    bar.addSuppressed(new IOException("baz", new InterruptedException("quux")));
    final Exception exception1 = new Exception("foo", bar);
    exception1.addSuppressed(new FoobarException());
    final Exception exception = exception1;

    final Path exceptionFile = temporaryFolder.getRoot().toPath().resolve("exception");
    Serialization.serialize(exception, exceptionFile);

    final Exception deserialized = Serialization.deserialize(exceptionFile);

    // Verify that stack trace can be accessed and printed and seems to be correct

    deserialized.printStackTrace();

    final StringWriter sw = new StringWriter();
    final PrintWriter pw = new PrintWriter(sw);
    deserialized.printStackTrace(pw);
    pw.flush();
    final String stacktrace = sw.toString();
    assertThat(stacktrace, containsString("java.lang.Exception: foo"));
    assertThat(stacktrace, containsString("at com.spotify.flo.SerializationTest.exceptionSerialization"));
    assertThat(stacktrace, containsString("Suppressed: com.spotify.flo.SerializationTest$FoobarException"));
    assertThat(stacktrace, containsString("Caused by: java.lang.RuntimeException: bar"));
    assertThat(stacktrace, containsString("Suppressed: java.io.IOException: baz"));
    assertThat(stacktrace, containsString("Caused by: java.lang.InterruptedException: quux"));

    assertThat(deserialized.getStackTrace().length, is(not(0)));
    assertThat(deserialized.getStackTrace()[0].getClassName(), is("com.spotify.flo.SerializationTest"));
    assertThat(deserialized.getStackTrace()[0].getMethodName(), is("exceptionSerialization"));
  }

  @Test
  public void serializeShouldPropagateSerializationExceptions() throws IOException {
    exception.expect(NotSerializableException.class);
    Serialization.serialize(new Object());
  }

  @Test
  public void serializeShouldPropagateIOException() throws Exception {
    exception.expect(NoSuchFileException.class);
    Serialization.serialize("foobar", Paths.get("non-existent-dir", "file"));
  }

  @Test
  public void deserializeShouldPropagateSerializationExceptions() throws IOException, ClassNotFoundException {
    exception.expect(StreamCorruptedException.class);
    Serialization.deserialize(new ByteArrayInputStream("foobar".getBytes()));
  }

  private static class FoobarException extends Exception {

  }

  private static SerializedLambda serializedLambda(Object o) throws ReflectiveOperationException {
    final Method m = o.getClass().getDeclaredMethod("writeReplace");
    m.setAccessible(true);
    return (SerializedLambda) m.invoke(o);
  }
}
