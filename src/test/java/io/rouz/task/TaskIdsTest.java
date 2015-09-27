package io.rouz.task;

import org.junit.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.junit.Assert.assertThat;

public class TaskIdsTest {

  @Test
  public void shouldHaveHumanReadableToString() {
    TaskId taskId = TaskIds.create("MyTask", "a", 1, 2.3, new Param(7));

    assertThat(taskId.toString(), startsWith("MyTask(a,1,2.3,Param{arg=7})"));
  }

  @Test
  public void shouldHaveIdentity() {
    TaskId taskId1 = TaskIds.create("MyTask", "a", 1, 2.3, new Param(7));
    TaskId taskId2 = TaskIds.create("MyTask", "a", 1, 2.3, new Param(7));

    assertThat(taskId1, not(sameInstance(taskId2)));
    assertThat(taskId1, equalTo(taskId2));
    assertThat(taskId2, equalTo(taskId1));
  }

  private static class Param {

    private final int arg;

    private Param(int arg) {
      this.arg = arg;
    }

    @Override
    public String toString() {
      return "Param{" +
             "arg=" + arg +
             '}';
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      Param param = (Param) o;

      return arg == param.arg;

    }

    @Override
    public int hashCode() {
      return arg;
    }
  }
}
