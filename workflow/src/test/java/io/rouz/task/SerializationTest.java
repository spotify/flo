package io.rouz.task;

import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.NotSerializableException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import io.rouz.task.TaskBuilder.F0;

import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class SerializationTest {

  transient File tempFile = tempFile();

  final String instanceField = "from instance";
  final TaskContext context = TaskContext.inmem();
  final AwaitingConsumer<String> val = new AwaitingConsumer<>();

  @Test
  public void shouldJavaUtilSerialize() throws Exception {
    Task<Long> task1 = Task.named("Foo", "Bar", 39).ofType(Long.class)
        .process(() -> 9999L);
    Task<String> task2 = Task.named("Baz", 40).ofType(String.class)
        .in(() -> task1)
        .ins(() -> singletonList(task1))
        .process((t1, t1l) -> t1l + " hello " + (t1 + 5));

    serialize(task2);
    Task<String> des = deserialize();
    context.evaluate(des).consume(val);

    assertEquals(des.id().name(), "Baz");
    assertEquals(val.awaitAndGet(), "[9999] hello 10004");
  }

  @Test(expected = NotSerializableException.class)
  public void shouldNotSerializeWithInstanceFieldReference() throws Exception {
    Task<String> task = Task.named("WithRef").ofType(String.class)
        .process(() -> instanceField + " causes an outer reference");

    serialize(task);
  }

  @Test
  public void shouldSerializeWithLocalReference() throws Exception {
    String local = instanceField;

    Task<String> task = Task.named("WithLocalRef").ofType(String.class)
        .process(() -> local + " won't cause an outer reference");

    serialize(task);
    Task<String> des = deserialize();
    context.evaluate(des).consume(val);

    assertEquals(val.awaitAndGet(), "from instance won't cause an outer reference");
  }

  @Test
  public void shouldSerializeWithMethodArgument() throws Exception {
    Task<String> task = closure(instanceField);

    serialize(task);
    Task<String> des = deserialize();
    context.evaluate(des).consume(val);

    assertEquals(val.awaitAndGet(), "from instance is enclosed");
  }

  private Task<String> closure(String arg) {
    return Task.named("Closed").ofType(String.class).process(() -> arg + " is enclosed");
  }

  @Test(expected = NotSerializableException.class)
  public void shouldNotSerializeAnonymousClass() throws Exception {
    Task<String> task = Task.named("WithAnonClass").ofType(String.class)
        .process(
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

  private <T> Task<T> deserialize() throws Exception {
    try (ObjectInputStream ois = new ObjectInputStream(new FileInputStream(tempFile))) {
      //noinspection unchecked
      return (Task<T>) ois.readObject();
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
