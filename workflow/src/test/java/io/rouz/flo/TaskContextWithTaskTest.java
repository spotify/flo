package io.rouz.flo;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;

import java.util.Optional;
import org.junit.Test;

public class TaskContextWithTaskTest {

  TaskContext delegate = mock(TaskContext.class);
  Task<?> task = Task.named("TestTAsk", "a", "b").ofType(String.class).process(() -> "");

  TaskContext sut = TaskContextWithTask.withTask(delegate, task);

  @Test
  public void testEmptyDefault() throws Exception {
    assertThat(TaskContext.inmem().currentTask(), is(Optional.empty()));
  }

  @Test
  public void testCurrentTaskId() throws Exception {
    assertThat(sut.currentTask(), is(Optional.of(task)));
  }
}
