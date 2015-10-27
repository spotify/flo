package io.rouz.task;

import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
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
    File tempFile = File.createTempFile("tempdata", ".bin");
    tempFile.deleteOnExit();

    TaskId id;

    try {
      Task<Long> task1 = Task.named("Foo", "Bar", 39)
          .process(() -> 9999L);
      Task<String> task2 = Task.named("Baz", 40)
          .in(() -> task1)
          .ins(() -> Stream.of(task1))
          .process((t1, t1l) -> t1l + " hello " + (t1 + 5));

      id = task1.id();

      FileOutputStream fos = new FileOutputStream(tempFile);
      ObjectOutputStream oos = new ObjectOutputStream(fos);
      oos.writeObject(task2);
      oos.close();
    } catch (Exception ex) {
      fail("Exception thrown during serialization: " + ex.toString());
      return;
    }

    try {
      FileInputStream fis = new FileInputStream(tempFile);
      ObjectInputStream ois = new ObjectInputStream(fis);
      Task<?> task = (Task<?>) ois.readObject();
      ois.close();

      List<Task<?>> deps = task.inputs().collect(toList());

      assertEquals(task.id().name(), "Baz");
      assertEquals(task.out(), "[9999] hello 10004");
      assertEquals(deps.get(0).id(), id);
    } catch (Exception ex) {
      fail("Exception thrown during deserialization: " + ex.toString());
    }
  }
}
