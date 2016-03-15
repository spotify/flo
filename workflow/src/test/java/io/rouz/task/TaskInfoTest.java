package io.rouz.task;

import org.junit.Test;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class TaskInfoTest {

  @Test
  public void testTaskInfo() throws Exception {
    Task<String> task = first();
    TaskInfo taskInfo = TaskInfo.ofTask(task);

    assertThat(taskInfo.id(), is(task.id()));
    assertThat(taskInfo.isReference(), is(false));
    assertThat(taskInfo.inputs().size(), is(1));

    TaskInfo input1 = taskInfo.inputs().get(0);
    assertThat(input1.id(), is(second(1).id()));
    assertThat(input1.isReference(), is(false));
    assertThat(input1.inputs().size(), is(1));

    TaskInfo input2 = input1.inputs().get(0);
    assertThat(input2.id(), is(second(1).id()));
    assertThat(input2.isReference(), is(true));
    assertThat(input2.inputs().size(), is(0));
  }

  private Task<String> first() {
    return Task.named("First")
        .in(() -> second(1))
        .process((s) -> "foo");
  }

  private Task<String> second(int i) {
    return Task.named("Second", i)
        .in(() -> second(i))
        .process((self) -> "bar");
  }
}
