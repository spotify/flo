package io.rouz.task.context;

import org.junit.Test;

import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import io.rouz.task.AwaitingConsumer;
import io.rouz.task.Task;
import io.rouz.task.TaskContext.Promise;
import io.rouz.task.TaskId;
import io.rouz.task.dsl.TaskBuilder;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

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

    AwaitingConsumer<Throwable> val = new AwaitingConsumer<>();
    new MyContext(Executors.newSingleThreadExecutor())
        .evaluate(task)
        .onFail(val);

    assertThat(val.awaitAndGet(), is(instanceOf(RuntimeException.class)));
    assertThat(val.awaitAndGet().getMessage(), is("failed"));
    assertThat(ran.get(), is(false));
    assertThat(count.get(), is(1));
  }

  private static class MyContext extends AsyncContext {

    ConcurrentMap<TaskId, Promise<?>> promises = new ConcurrentHashMap<>();

    protected MyContext(ExecutorService executor) {
      super(executor);
    }

    @Override
    public <T> Value<T> evaluate(Task<T> task) {
      //noinspection unchecked
      return (Value<T>) promises.computeIfAbsent(task.id(), (ˍ) -> {
        final Promise<T> promise = promise();
        final Value<T> evaluate = super.evaluate(task);

        evaluate.consume(promise::set);
        evaluate.onFail(promise::fail);

        return promise;
      }).value();
    }

    @Override
    public <T> Value<T> invokeProcessFn(TaskId taskId, TaskBuilder.F0<Value<T>> processFn) {
      final Promise<T> promise = promise();
      final Value<T> tValue = processFn.get();

      tValue.consume(promise::set);
      tValue.onFail(promise::fail);

      return promise.value();
    }
  }
}
