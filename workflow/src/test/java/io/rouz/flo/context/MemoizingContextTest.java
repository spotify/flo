package io.rouz.flo.context;

import com.google.auto.value.AutoValue;

import org.junit.Before;
import org.junit.Test;

import java.util.Optional;

import io.rouz.flo.AwaitingConsumer;
import io.rouz.flo.Task;
import io.rouz.flo.TaskContext;

import static io.rouz.flo.TaskContext.inmem;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class MemoizingContextTest {

  static ExampleValue value;
  static MemoizingContext.Memoizer<ExampleValue> memoizer;

  TaskContext context = MemoizingContext.composeWith(inmem());

  int countUpstreamRuns = 0;
  int countExampleRuns = 0;

  @Before
  public void setUp() throws Exception {
    value = null;
    memoizer = new ExampleValueMemoizer();
  }

  @Test
  public void evaluatesAndStores() throws Exception {
    Task<ExampleValue> example = example(7);

    AwaitingConsumer<ExampleValue> await = new AwaitingConsumer<>();
    context.evaluate(example).consume(await);

    ExampleValue evaluatedValue = await.awaitAndGet();
    assertThat(evaluatedValue, is(val("ups7", 7)));
    assertThat(countUpstreamRuns, is(1));
    assertThat(countExampleRuns, is(1));
    assertThat(value, is(val("ups7", 7)));
  }

  @Test
  public void evaluatesAndStoresWithExplicitMemoizer() throws Exception {
    context = MemoizingContext.builder(inmem())
        .memoizer(memoizer)
        .build();

    Task<ExampleValue> example = example(7);

    AwaitingConsumer<ExampleValue> await = new AwaitingConsumer<>();
    context.evaluate(example).consume(await);

    ExampleValue evaluatedValue = await.awaitAndGet();
    assertThat(evaluatedValue, is(val("ups7", 7)));
    assertThat(countUpstreamRuns, is(1));
    assertThat(countExampleRuns, is(1));
    assertThat(value, is(val("ups7", 7)));
  }

  @Test
  public void shortsEvaluationIfMemoizedValueExists() throws Exception {
    value = val("ups8", 8);

    Task<ExampleValue> example = example(8);

    AwaitingConsumer<ExampleValue> await = new AwaitingConsumer<>();
    context.evaluate(example).consume(await);

    ExampleValue evaluatedValue = await.awaitAndGet();
    assertThat(evaluatedValue, is(val("ups8", 8)));
    assertThat(countUpstreamRuns, is(0));
    assertThat(countExampleRuns, is(0));
  }

  Task<String> upstream(int i ) {
    return Task.named("upstream", i).ofType(String.class)
        .process(() -> {
          countUpstreamRuns++;
          return "ups" + i;
        });
  }

  Task<ExampleValue> example(int i) {
    return Task.named("example", i).ofType(ExampleValue.class)
        .in(() -> upstream(i))
        .in(() -> upstream(i))
        .process((ups, ups1) -> {
          countExampleRuns++;
          return val(ups, i);
        });
  }

  @AutoValue
  public abstract static class ExampleValue {
    abstract String foo();
    abstract int bar();

    @MemoizingContext.Memoizer.Impl
    public static MemoizingContext.Memoizer<ExampleValue> memoizer() {
      return memoizer;
    }
  }

  static ExampleValue val(String foo, int bar) {
    return new AutoValue_MemoizingContextTest_ExampleValue(foo, bar);
  }

  static class ExampleValueMemoizer implements MemoizingContext.Memoizer<ExampleValue> {

    @Override
    public Optional<ExampleValue> lookup(Task task) {
      System.out.println("lookup task " + task.id());
      return Optional.ofNullable(MemoizingContextTest.value);
    }

    @Override
    public void store(Task task, ExampleValue value) {
      System.out.println("store task " + task.id() + " value = " + value);
      MemoizingContextTest.value = value;
    }
  }
}
