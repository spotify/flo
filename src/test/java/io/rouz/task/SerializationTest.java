package io.rouz.task;

import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.List;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class SerializationTest {

  @Test
  public void shouldJavaUtilSerialize() throws Exception {
    File tempFile = tempFile();

    Task<Long> task1 = Task.named("Foo", "Bar", 39)
        .process(() -> 9999L);
    Task<String> task2 = Task.named("Baz", 40)
        .in(() -> task1)
        .ins(() -> Stream.of(task1))
        .process((t1, t1l) -> t1l + " hello " + (t1 + 5));

    serialize(task2, tempFile);
    Task<?> des = deserialize(tempFile);

    List<Task<?>> deps = des.inputs().collect(toList());

    assertEquals(des.id().name(), "Baz");
    assertEquals(des.out(), "[9999] hello 10004");
    assertEquals(deps.get(0).id(), task1.id());
  }

  @Test
  public void shouldWorkWithEnclosedValues() throws Exception {
    File tempFile = tempFile();

    Task<String> task = closure("woop");

    serialize(task, tempFile);
    Task<?> des = deserialize(tempFile);

    assertEquals(des.out(), "woop is enclosed");
  }

  private Task<String> closure(String arg) {
    return Task.named("Closed")
        .process(() -> arg + " is enclosed");
  }

  private void serialize(Task<?> task, File file) {
    try {
      FileOutputStream fos = new FileOutputStream(file);
      ObjectOutputStream oos = new ObjectOutputStream(fos);
      oos.writeObject(task);
      oos.close();
    } catch (Exception ex) {
      fail("Exception thrown during serialization: " + ex.toString());
    }
  }

  private Task<?> deserialize(File file) {
    try {
      FileInputStream fis = new FileInputStream(file);
      ObjectInputStream ois = new ObjectInputStream(fis);
      Task<?> task = (Task<?>) ois.readObject();
      ois.close();

      return task;
    } catch (Exception ex) {
      fail("Exception thrown during deserialization: " + ex.toString());
    }

    throw new IllegalStateException("should not reach");
  }

  private File tempFile() throws IOException {
    File tempFile = File.createTempFile("tempdata", ".bin");
    tempFile.deleteOnExit();
    return tempFile;
  }
}
