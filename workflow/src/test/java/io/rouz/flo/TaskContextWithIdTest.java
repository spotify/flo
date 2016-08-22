package io.rouz.flo;

import org.junit.Test;

import java.util.Optional;
import java.util.stream.Collector;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TaskContextWithIdTest {

  static final Task<String> TASK = Task.create(() -> "", String.class, "foo");
  static final Fn<TaskContext.Value<Object>> FN = () -> null;

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

  @Test
  public void testEvaluateDelegates() throws Exception {
    //noinspection unchecked
    TaskContext.Value<String> value = mock(TaskContext.Value.class);
    when(delegate.evaluate(TASK)).thenReturn(value);

    assertThat(sut.evaluate(TASK), is(value));
  }

  @Test
  public void testInvokeProcessFnDelegates() throws Exception {
    //noinspection unchecked
    TaskContext.Value<Object> value = mock(TaskContext.Value.class);
    when(delegate.invokeProcessFn(TASK.id(), FN)).thenReturn(value);

    assertThat(sut.invokeProcessFn(TASK.id(), FN), is(value));
  }

  @Test
  public void testValueDelegates() throws Exception {
    //noinspection unchecked
    TaskContext.Value<Object> value = mock(TaskContext.Value.class);
    Fn<Object> fn = Object::new;
    when(delegate.value(fn)).thenReturn(value);

    sut.value(fn);
  }

  @Test
  public void testImmediateValueDelegates() throws Exception {
    //noinspection unchecked
    TaskContext.Value<String> value = mock(TaskContext.Value.class);
    when(delegate.immediateValue("STRING")).thenReturn(value);

    assertThat(sut.immediateValue("STRING"), is(value));
  }

  @Test
  public void testPromiseDelegates() throws Exception {
    //noinspection unchecked
    TaskContext.Promise<String> promise = mock(TaskContext.Promise.class);
    when(delegate.<String>promise()).thenReturn(promise);

    assertThat(sut.promise(), is(promise));
  }

  @Test
  public void testToValueListDelegates() throws Exception {
    Collector collector = TaskContext.inmem().toValueList();
    //noinspection unchecked
    when(delegate.toValueList()).thenReturn(collector);

    // todo: this method does not really belong to the interface
    assertThat(sut.toValueList(), is(collector));
  }
}
