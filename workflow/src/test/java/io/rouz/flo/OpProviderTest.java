package io.rouz.flo;

import static io.rouz.flo.TestUtils.evalAndGet;
import static io.rouz.flo.TestUtils.evalAndGetException;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.spy;

import org.junit.Test;
import org.mockito.InOrder;

public class OpProviderTest {

  private String setFromInjected;

  @Test
  public void injectsOps() throws Exception {
    Task<String> task = Task.named("inject").ofType(String.class)
        .op(new TestProvider())
        .process(injected -> injected.doSomething("foo"));

    String result = evalAndGet(task);

    assertThat(result, is("ok"));
    assertThat(setFromInjected, is("something foo"));
  }

  @Test
  public void injectsOpsSecond() throws Exception {
    Task<String> task = Task.named("inject").ofType(String.class)
        .in(() -> Task.named("foo").ofType(String.class).process(() -> "hej"))
        .op(new TestProvider())
        .process((a, b) -> b.doSomething("bar"));

    String result = evalAndGet(task);

    assertThat(result, is("ok"));
    assertThat(setFromInjected, is("something bar"));
  }

  @Test
  public void lifecycleMethodsCalledInOrder() throws Exception {
    BasicProvider op1 = spy(new BasicProvider("foo"));
    BasicProvider op2 = spy(new BasicProvider("bar"));
    Task<String> task = Task.named("inject").ofType(String.class)
        .op(op1)
        .op(op2)
        .process((i1, i2) -> {
          assertThat(i1, is("foo"));
          assertThat(i2, is("bar"));
          op1.mark();
          return i1 + i2;
        });

    evalAndGet(task);
    InOrder inOrder = inOrder(op1, op2);
    inOrder.verify(op1).preRun(task);
    inOrder.verify(op2).preRun(task);
    inOrder.verify(op1).mark();
    inOrder.verify(op2).onSuccess(task, "foobar");
    inOrder.verify(op1).onSuccess(task, "foobar");
  }

  @Test
  public void lifecycleMethodsCalledInOrderOnFail() throws Exception {
    BasicProvider op1 = spy(new BasicProvider("foo"));
    BasicProvider op2 = spy(new BasicProvider("bar"));
    Task<String> task = Task.named("inject").ofType(String.class)
        .op(op1)
        .op(op2)
        .process((i1, i2) -> {
          assertThat(i1, is("foo"));
          assertThat(i2, is("bar"));
          op1.mark();
          throw new RuntimeException("force fail");
        });

    Throwable throwable = evalAndGetException(task);
    InOrder inOrder = inOrder(op1, op2);
    inOrder.verify(op1).preRun(task);
    inOrder.verify(op2).preRun(task);
    inOrder.verify(op1).mark();
    inOrder.verify(op2).onFail(task, throwable);
    inOrder.verify(op1).onFail(task, throwable);
    assertThat(throwable.getMessage(), is("force fail"));
  }

  private class Injected {
    String doSomething(String some) {
      setFromInjected = "something " + some;
      return "ok";
    }
  }

  private class TestProvider implements OpProvider<Injected> {

    @Override
    public Injected provide(TaskContext taskContext) {
      return new Injected();
    }
  }

  private class BasicProvider implements OpProvider<String> {

    private final String inject;

    private BasicProvider(String inject) {
      this.inject = inject;
    }

    @Override
    public String provide(TaskContext taskContext) {
      return inject;
    }

    public void mark() {
      // noop, used to verify call order
    }
  }
}
