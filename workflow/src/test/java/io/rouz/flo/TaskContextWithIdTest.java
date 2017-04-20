package io.rouz.flo;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;

import java.util.Optional;
import org.junit.Test;

public class TaskContextWithIdTest {

  TaskContext delegate = mock(TaskContext.class);
  TaskId taskId = TaskId.create("TestTAsk", "a", "b");

  TaskContext sut = TaskContextWithId.withId(delegate, taskId);

  @Test
  public void testEmptyDefault() throws Exception {
    assertThat(TaskContext.inmem().currentTaskId(), is(Optional.empty()));
  }

  @Test
  public void testCurrentTaskId() throws Exception {
    assertThat(sut.currentTaskId(), is(Optional.of(taskId)));
  }
}
