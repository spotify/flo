package io.rouz.flo.context;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.typeCompatibleWith;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import io.rouz.flo.Fn;
import io.rouz.flo.Task;
import io.rouz.flo.TaskContext;
import io.rouz.flo.TaskContext.Value;
import io.rouz.flo.TaskContextWithTask;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class ForwardingTaskContextTest {

  static final Task<String> TASK = Task.create(() -> "", String.class, "test");

  TaskContext delegate = mock(TaskContext.class);
  TaskContext sut = new TestContext(delegate);

  @Test
  public void evaluate() throws Exception {
    sut.evaluate(TASK);

    ArgumentCaptor<TaskContext> contextArgumentCaptor = ArgumentCaptor.forClass(TaskContext.class);
    verify(delegate).evaluateInternal(eq(TASK), contextArgumentCaptor.capture());

    TaskContext capturedContext = contextArgumentCaptor.getValue();
    assertThat(capturedContext.getClass(), typeCompatibleWith(TaskContextWithTask.class));

    TaskContextWithTask wrapperContext = (TaskContextWithTask) capturedContext;
    assertThat(wrapperContext.delegate, is(sut));
  }

  @Test
  public void evaluateInternal() throws Exception {
    sut.evaluateInternal(TASK, delegate);

    verify(delegate).evaluateInternal(TASK, delegate);
  }

  @Test
  public void invokeProcessFn() throws Exception {
    Fn<Value<Object>> fn = () -> null;
    sut.invokeProcessFn(TASK.id(), fn);

    verify(delegate).invokeProcessFn(TASK.id(), fn);
  }

  @Test
  public void value() throws Exception {
    Fn<String> fn = () -> "";
    sut.value(fn);

    verify(delegate).value(fn);
  }

  @Test
  public void immediateValue() throws Exception {
    String value = "";
    sut.immediateValue(value);

    verify(delegate).immediateValue(value);
  }

  @Test
  public void promise() throws Exception {
    sut.promise();

    verify(delegate).promise();
  }

  private static class TestContext extends ForwardingTaskContext {

    TestContext(TaskContext delegate) {
      super(delegate);
    }
  }
}
