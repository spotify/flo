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

import static java.util.Collections.singletonList;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertEquals;

import com.spotify.flo.TaskBuilder.F0;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.NotSerializableException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class SerializationTest {

  @Rule public ExpectedException exception = ExpectedException.none();

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

  private byte[] serialize(Task<?> task) throws Exception{
    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try (ObjectOutputStream oos = new ObjectOutputStream(baos)) {
      oos.writeObject(task);
    }
    return baos.toByteArray();
  }

  private <T> Task<T> deserialize(byte[] bytes) throws Exception {
    try (ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(bytes))) {
      //noinspection unchecked
      return (Task<T>) ois.readObject();
    }
  }
}
