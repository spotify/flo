package io.rouz.task;

import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.NotSerializableException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import io.rouz.task.dsl.TaskBuilder.F0;

import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class SerializationTest {

  transient File tempFile = tempFile();

  final String instanceField = "from instance";

  @Test
  public void shouldJavaUtilSerialize() throws Exception {
    Task<Long> task1 = Task.named("Foo", "Bar", 39)
        .constant(() -> 9999L);
    Task<String> task2 = Task.named("Baz", 40)
        .in(() -> task1)
        .ins(() -> singletonList(task1))
        .process((t1, t1l) -> t1l + " hello " + (t1 + 5));

    serialize(task2);
    Task<?> des = deserialize();

    assertEquals(des.id().name(), "Baz");
    assertEquals(des.out(), "[9999] hello 10004");
  }

  @Test(expected = NotSerializableException.class)
  public void shouldNotSerializeWithInstanceFieldReference() throws Exception {
    Task<String> task = Task.named("WithRef")
        .constant(() -> instanceField + " causes an outer reference");

    serialize(task);
  }

  @Test
  public void shouldSerializeWithLocalReference() throws Exception {
    String local = instanceField;

    Task<String> task = Task.named("WithLocalRef")
        .constant(() -> local + " won't cause an outer reference");

    serialize(task);
    Task<?> des = deserialize();

    assertEquals(des.out(), "from instance won't cause an outer reference");
  }

  @Test
  public void shouldSerializeWithMethodArgument() throws Exception {
    Task<String> task = closure(instanceField);

    serialize(task);
    Task<?> des = deserialize();

    assertEquals(des.out(), "from instance is enclosed");
  }

  private Task<String> closure(String arg) {
    return Task.create(() -> arg + " is enclosed", "Closed");
  }

  @Test(expected = NotSerializableException.class)
  public void shouldNotSerializeAnonymousClass() throws Exception {
    Task<String> task = Task.named("WithAnonClass")
        .constant(
            new F0<String>() {
              @Override
              public String get() {
                return "yes? no!";
              }
            });

    serialize(task);
  }

  private void serialize(Task<?> task) throws Exception{
    try (ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(tempFile))) {
      oos.writeObject(task);
    }
  }

  private Task<?> deserialize() throws Exception {
    try (ObjectInputStream ois = new ObjectInputStream(new FileInputStream(tempFile))) {
      return (Task<?>) ois.readObject();
    }
  }

  private File tempFile() {
    try {
      File tempFile = File.createTempFile("tempdata", ".bin");
      tempFile.deleteOnExit();
      return tempFile;
    } catch (IOException e) {
      fail("Could not create temp file");
    }

    throw new IllegalStateException("should not reach");
  }
}
