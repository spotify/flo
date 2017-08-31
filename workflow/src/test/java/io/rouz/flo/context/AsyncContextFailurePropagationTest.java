package io.rouz.flo.context;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import io.rouz.flo.AwaitValue;
import io.rouz.flo.Task;
import io.rouz.flo.TaskContext;
import io.rouz.flo.TaskContext.Promise;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Test;

public class AsyncContextFailurePropagationTest {

  @Test
  public void shouldFailTaskIfUpstreamsFail() throws Exception {
    Task<String> failingUpstream = Task.named("Failing").ofType(String.class)
        .processWithContext(tc -> {
          final Promise<String> promise = tc.promise();
          promise.fail(new RuntimeException("failed"));
          return promise.value();
        });

    AtomicInteger count = new AtomicInteger(0);
    Task<String> successfulUpstream = Task.named("Succeeding").ofType(String.class)
        .process(() -> {
          count.incrementAndGet();
          return "foo";
        });

    AtomicBoolean ran = new AtomicBoolean(false);
    Task<String> task = Task.named("Dependent").ofType(String.class)
        .ins(() -> Arrays.asList(successfulUpstream, failingUpstream))
        .in(() -> successfulUpstream)
        .in(() -> failingUpstream)
        .process((a, b, c) -> {
          ran.set(true);
          return c + b + a;
        });

    AwaitValue<Throwable> val = new AwaitValue<>();
    MemoizingContext.composeWith(TaskContext.inmem())
        .evaluate(task)
        .onFail(val);

    assertThat(val.awaitAndGet(), is(instanceOf(RuntimeException.class)));
    assertThat(val.awaitAndGet().getMessage(), is("failed"));
    assertThat(ran.get(), is(false));
    assertThat(count.get(), is(1));
  }
}
