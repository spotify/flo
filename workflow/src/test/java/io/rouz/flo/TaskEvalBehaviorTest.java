package io.rouz.flo;

import static io.rouz.flo.TestUtils.evalAndGet;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import io.rouz.flo.TaskBuilder.F1;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.junit.Test;

/**
 * Tests that verify the interaction between {@link Task} instances and the {@link TaskContext}.
 */
public class TaskEvalBehaviorTest {

  @Test
  public void shouldRunAsExpected() throws Exception {
    Task<EvenResult> wasEven = isEven(6);
    Task<EvenResult> madeEven = isEven(5);

    assertThat(evalAndGet(wasEven), instanceOf(WasEven.class));
    assertThat(evalAndGet(wasEven).result(), is(6));
    assertThat(evalAndGet(madeEven), instanceOf(MadeEven.class));
    assertThat(evalAndGet(madeEven).result(), is(10));
  }

  @Test
  public void shouldHandleStreamParameters() throws Exception {
    Fn<Task<Integer>> countSupplier = countConstructor();

    // 1,2,3,4,5
    List<Task<Integer>> fiveInts = Stream
        .generate(countSupplier)
        .limit(5)
        .collect(toList());

    Task<Integer> sum = Task.named("Sum").ofType(Integer.class)
        .ins(() -> fiveInts)
        .process(this::sumInts);

    // 1+2+3+4+5 = 15
    assertThat(evalAndGet(sum), is(15));
  }

  @Test
  public void shouldHandleMixedStreamAndPlainParameters() throws Exception {
    Fn<Task<Integer>> countSupplier = countConstructor();

    // 1,2,3,4,5
    List<Task<Integer>> fiveInts = Stream
        .generate(countSupplier)
        .limit(5)
        .collect(toList());

    Task<Integer> sum = Task.named("Sum").ofType(Integer.class)
        .in(() -> isEven(5))
        .ins(() -> fiveInts)
        .in(() -> isEven(2))
        .process((a, ints, b) -> a.result() + sumInts(ints) + b.result());

    // (5*2) + (1+2+3+4+5) + 2 = 27
    assertThat(evalAndGet(sum), is(27));
  }

  @Test
  public void shouldHandleMultipleStreamParameters() throws Exception {
    Fn<Task<Integer>> countSupplier = countConstructor();

    Fn<List<Task<Integer>>> fiveInts = () -> Stream
        .generate(countSupplier)
        .limit(5)
        .collect(toList());

    Task<Integer> sum = Task.named("Sum").ofType(Integer.class)
        .ins(fiveInts)
        .ins(fiveInts)
        .process((first5, second5) -> sumInts(first5) + sumInts(second5));

    // (1+2+3+4+5) + (6+7+8+9+10) = 55
    assertThat(evalAndGet(sum), is(55));
  }

  @Test
  public void shouldOnlyEvaluateInputsParameterOnce() throws Exception {
    Fn<Task<Integer>> countSupplier = countConstructor();

    Task<Integer> sum = Task.named("Sum").ofType(Integer.class)
        .in(countSupplier)
        .in(countSupplier)
        .in(countSupplier)
        .process((a, b, c) -> a + b + c);

    // dummy run
    evalAndGet(sum);

    // 1+2+3 = 6
    assertThat(evalAndGet(sum), is(6));
  }

  @Test
  public void shouldOnlyEvaluateStreamParameterOnce() throws Exception {
    Fn<Task<Integer>> countSupplier = countConstructor();

    Fn<List<Task<Integer>>> fiveInts = () -> Stream
        .generate(countSupplier)
        .limit(5)
        .collect(toList());

    Task<Integer> sum = Task.named("Sum").ofType(Integer.class)
        .ins(fiveInts)
        .ins(fiveInts)
        .ins(fiveInts)
        .process((first5, second5, third5) -> sumInts(first5) + sumInts(second5) + sumInts(third5));

    // dummy run
    evalAndGet(sum);

    // (1+2+3+4+5) + (6+7+8+9+10) + (11+12+13+14+15) = 120
    assertThat(evalAndGet(sum), is(120));
  }

  @Test
  public void shouldListInputIds() throws Exception {
    Task<String> top = Task.named("Top").ofType(String.class)
        .in(() -> isEven(0))
        .in(() -> isEven(1))
        .in(() -> isEven(3))
        .process((a, b, c) -> "done");

    List<TaskId> inputs = top.inputs().stream().map(Task::id).collect(toList());

    TaskId isEven0Id = isEven(0).id();
    TaskId isEven1Id = isEven(1).id();
    TaskId isEven3Id = isEven(3).id();

    assertThat(inputs, containsInOrder(isEven0Id, isEven1Id));
    assertThat(inputs, containsInOrder(isEven0Id, isEven3Id));
    assertThat(inputs, containsInOrder(isEven1Id, isEven3Id));
  }

  @Test
  public void shouldListInputsLazily() throws Exception {
    Fn<Task<Integer>> countSupplier = countConstructor();

    Task<Integer> sum = Task.named("Sum").ofType(Integer.class)
        .in(countSupplier)
        .in(countSupplier)
        .in(countSupplier)
        .process((a, b, c) -> a + b + c);

    // pre fetch one
    Task<Integer> one = countSupplier.get();

    // both run and and get inputs
    evalAndGet(sum);
    List<TaskId> inputs = sum.inputs().stream().map(Task::id).collect(toList());

    assertThat(inputs.get(0).toString(), startsWith("Count(4)"));
    assertThat(inputs.get(1).toString(), startsWith("Count(3)"));
    assertThat(inputs.get(2).toString(), startsWith("Count(2)"));

    assertThat(evalAndGet(one), is(1));
    assertThat(evalAndGet(sum), is(9)); // 2+3+4 = 9
  }

  @Test
  public void shouldLinearizeTasks() throws Exception {
    Task<String> top = Task.named("Top").ofType(String.class)
        .in(() -> isEven(0))
        .in(() -> isEven(1))
        .process((a, b) -> "done");

    List<TaskId> taskIds = top.inputsInOrder()
        .map(Task::id)
        .collect(toList());

    TaskId evenify1Id = evenify(1).id();
    TaskId isEven1Id = isEven(1).id();

    assertThat(taskIds, containsInOrder(evenify1Id, isEven1Id));
  }

  @Test
  public void shouldFlattenStreamParameters() throws Exception {
    Task<String> top = Task.named("Top").ofType(String.class)
        .ins(() -> asList(isEven(0), isEven(1)))
        .process(results -> "done " + results.size());

    List<TaskId> taskIds = top.inputsInOrder()
        .map(Task::id)
        .collect(toList());

    TaskId isEven1Id = isEven(1).id();
    TaskId evenify1Id = evenify(1).id();

    assertThat(taskIds.size(), is(3));
    assertThat(taskIds, containsInOrder(evenify1Id, isEven1Id));
  }

  @Test
  public void shouldLinearizeMixedStreamAndPlainParameters() throws Exception {
    F1<Integer, Task<Integer>> evenResult = n ->
        Task.named("EvenResult", n).ofType(Integer.class)
            .in(() -> isEven(n))
            .process(EvenResult::result);

    Task<Integer> sum = Task.named("Sum").ofType(Integer.class)
        .in(() -> isEven(5))
        .ins(() -> asList(evenResult.apply(0), evenResult.apply(1)))
        .ins(() -> singletonList(evenResult.apply(3)))
        .process((a, ints, b) -> a.result() + sumInts(ints) + sumInts(b));

    List<TaskId> taskIds = sum.inputsInOrder()
        .map(Task::id)
        .collect(toList());

    TaskId evenify5Id = evenify(5).id();
    TaskId evenify1Id = evenify(1).id();
    TaskId evenify3Id = evenify(3).id();

    assertThat(taskIds.size(), is(10));
    assertThat(taskIds, containsInOrder(evenify5Id, evenify1Id));
    assertThat(taskIds, containsInOrder(evenify5Id, evenify3Id));
    assertThat(taskIds, containsInOrder(evenify1Id, evenify3Id));
  }

  @Test
  public void shouldGetTaskIdFromContext0() throws Exception {
    Task<Task> task = Task.named("MyOwnId").ofType(Task.class)
        .processWithContext(tc -> tc.immediateValue(tc.currentTask().get()));

    validateReturnsOwnTask(task);
  }

  @Test
  public void shouldGetTaskIdFromContext1() throws Exception {
    Task<Task> task = Task.named("MyOwnId").ofType(Task.class)
        .in(() -> leaf("A"))
        .processWithContext((tc, a) -> tc.immediateValue(tc.currentTask().get()));

    validateReturnsOwnTask(task);
  }

  @Test
  public void shouldGetTaskIdFromContext2() throws Exception {
    Task<Task> task = Task.named("MyOwnId").ofType(Task.class)
        .in(() -> leaf("A"))
        .in(() -> leaf("B"))
        .processWithContext((tc, a, b) -> tc.immediateValue(tc.currentTask().get()));

    validateReturnsOwnTask(task);
  }

  @Test
  public void shouldGetTaskIdFromContext3() throws Exception {
    Task<Task> task = Task.named("MyOwnId").ofType(Task.class)
        .in(() -> leaf("A"))
        .in(() -> leaf("B"))
        .in(() -> leaf("C"))
        .processWithContext((tc, a, b, c) -> tc.immediateValue(tc.currentTask().get()));

    validateReturnsOwnTask(task);
  }

  @Test
  public void shouldGetTaskIdFromContextNested() throws Exception {
    AwaitValue<Task> inner = new AwaitValue<>();
    Task<String> innerTask = Task.named("Inner").ofType(String.class)
        .processWithContext(tc -> {
          inner.accept(tc.currentTask().get());
          return tc.immediateValue("");
        });
    Task<Task> task = Task.named("MyOwnId").ofType(Task.class)
        .in(() -> leaf("A"))
        .in(() -> innerTask)
        .in(() -> leaf("C"))
        .processWithContext((tc, a, b, c) -> tc.immediateValue(tc.currentTask().get()));

    validateReturnsOwnTask(task);
    assertThat(inner.awaitAndGet(), is(innerTask));
  }

  private void validateReturnsOwnTask(Task<Task> task) throws InterruptedException {
    AwaitValue<Task> val = new AwaitValue<>();
    TaskContext.inmem()
        .evaluate(task)
        .consume(val);

    assertThat(val.awaitAndGet(), is(task));
  }

  @Test
  public void shouldEvaluateInputsInParallelForChainedTask() throws Exception {
    AtomicBoolean processed = new AtomicBoolean(false);
    Task<String> task = Task.named("WithInputs").ofType(String.class)
        .in(() -> leaf("A"))
        .in(() -> leaf("B"))
        .in(() -> leaf("C"))
        .process((a, b, c) -> {
          processed.set(true);
          return "done: " + a + b + c;
        });

    validateParallelEvaluation(task, processed);
  }

  private void validateParallelEvaluation(Task<String> task, AtomicBoolean processed)
      throws InterruptedException {

    ControlledBlockingContext context = new ControlledBlockingContext();
    context.evaluate(task);

    // first wait for the main task to be in progress then release it
    // to trigger upstreams to start evaluating
    context.waitFor(task);
    context.release(task);

    context.waitUntilNumConcurrent(4); // {WithInputs, A, B, C}
    assertTrue(context.isWaiting(leaf("A")));
    assertTrue(context.isWaiting(leaf("B")));
    assertTrue(context.isWaiting(leaf("C")));
    assertFalse(processed.get());

    context.release(leaf("C"));
    context.waitUntilNumConcurrent(3); // {WithInputs, A, B}
    assertTrue(context.isWaiting(leaf("A")));
    assertTrue(context.isWaiting(leaf("B")));
    assertFalse(context.isWaiting(leaf("C")));

    context.release(leaf("B"));
    context.release(leaf("A"));
    context.waitUntilNumConcurrent(0); // WithInputs will also complete here
    assertTrue(processed.get());
  }

  @Test
  public void shouldInterceptProcessFunctionInContext0() throws Exception {
    Task<String> top = Task.named("Top").ofType(String.class)
        .process(() -> "done");

    validateInterception(top, "done");
  }

  @Test
  public void shouldInterceptProcessFunctionInContext1() throws Exception {
    Task<String> top = Task.named("Top").ofType(String.class)
        .in(() -> leaf("A"))
        .process((a) -> "done");

    validateInterception(top, "done", leaf("A"));
  }

  @Test
  public void shouldInterceptProcessFunctionInContext1L() throws Exception {
    Task<String> top = Task.named("Top").ofType(String.class)
        .ins(() -> singletonList(leaf("A")))
        .process((a) -> "done");

    validateInterception(top, "done", leaf("A"));
  }

  @Test
  public void shouldInterceptProcessFunctionInContext2() throws Exception {
    Task<String> top = Task.named("Top").ofType(String.class)
        .in(() -> leaf("A"))
        .in(() -> leaf("B"))
        .process((a, b) -> "done");

    validateInterception(top, "done", leaf("A"), leaf("B"));
  }

  @Test
  public void shouldInterceptProcessFunctionInContext2L() throws Exception {
    Task<String> top = Task.named("Top").ofType(String.class)
        .ins(() -> singletonList(leaf("A")))
        .ins(() -> singletonList(leaf("B")))
        .process((a, b) -> "done");

    validateInterception(top, "done", leaf("A"), leaf("B"));
  }

  @Test
  public void shouldInterceptProcessFunctionInContext3() throws Exception {
    Task<String> top = Task.named("Top").ofType(String.class)
        .in(() -> leaf("A"))
        .in(() -> leaf("B"))
        .in(() -> leaf("C"))
        .process((a, b, c) -> "done");

    validateInterception(top, "done", leaf("A"), leaf("B"), leaf("C"));
  }

  @Test
  public void shouldInterceptProcessFunctionInContext0C() throws Exception {
    Task<String> top = Task.named("Top").ofType(String.class)
        .processWithContext((tc) -> tc.immediateValue("done"));

    validateInterception(top, "done");
  }

  @Test
  public void shouldInterceptProcessFunctionInContextC() throws Exception {
    Task<String> top = Task.named("Top").ofType(String.class)
        .in(() -> leaf("A"))
        .in(() -> leaf("B"))
        .processWithContext((tc, a, b) -> tc.immediateValue("done"));

    validateInterception(top, "done", leaf("A"), leaf("B"));
  }

  @Test
  public void shouldInterceptProcessFunctionInContextCL() throws Exception {
    Task<String> top = Task.named("Top").ofType(String.class)
        .ins(() -> singletonList(leaf("A")))
        .ins(() -> singletonList(leaf("B")))
        .processWithContext((tc, a, b) -> tc.immediateValue("done"));

    validateInterception(top, "done", leaf("A"), leaf("B"));
  }

  private void validateInterception(Task<String> task, String res, Task<?>... inputs)
      throws Exception {
    AtomicBoolean intercepted = new AtomicBoolean(false);

    // gating mechanism used in ControlledBlockingContext to implement intercepts
    ControlledBlockingContext context = new ControlledBlockingContext();
    context.intercept(task, valueFn -> {
      intercepted.set(true);
      return valueFn.get().map(done -> "!!" + done + "!!");
    });

    AwaitValue<String> val = new AwaitValue<>();
    context.evaluate(task).consume(val);

    context.waitFor(task);
    context.release(task);

    // release inputs one at a time and verify interception does not happen
    for (int i = 0; i < inputs.length; i++) {
      context.waitUntilNumConcurrent(1 + inputs.length - i);
      for (int j = i; j < inputs.length; j++) {
        assertThat(context.waitingTasks(), hasItem(inputs[j].id()));
      }
      assertFalse(intercepted.get());

      context.release(inputs[i]);
    }

    assertThat(val.awaitAndGet(), is("!!" + res + "!!"));
    assertTrue(intercepted.get());
  }

  private Task<String> leaf(String s) {
    return Task.named("Leaf", s).ofType(String.class).process(() -> s);
  }

  private Fn<Task<Integer>> countConstructor() {
    AtomicInteger counter = new AtomicInteger(0);
    return () -> {
      int n = counter.incrementAndGet();
      return Task.named("Count", n).ofType(Integer.class)
          .process(() -> n);
    };
  }

  private int sumInts(List<Integer> intsList) {
    return intsList.stream().reduce(0, (a, b) -> a + b);
  }

  private Task<EvenResult> isEven(int n) {
    TaskBuilder<EvenResult> isEven = Task.named("IsEven", n).ofType(EvenResult.class);

    if (n % 2 == 0) {
      return isEven.process(() -> new WasEven(n));
    }

    return isEven
        .in(() -> evenify(n))
        .process(MadeEven::new);
  }

  private Task<Integer> evenify(int n) {
    return Task.named("Evenify", n).ofType(Integer.class)
        .process(() -> n * 2);
  }

  // Result ADT
  abstract class EvenResult {

    private final int result;

    EvenResult(int result) {
      this.result = result;
    }

    int result() {
      return result;
    }
  }

  class WasEven extends EvenResult {

    WasEven(int result) {
      super(result);
    }
  }

  class MadeEven extends EvenResult {

    MadeEven(int result) {
      super(result);
    }
  }

  private static <T> Matcher<Iterable<? extends T>> containsInOrder(T a, T b) {
    Objects.requireNonNull(a);
    Objects.requireNonNull(b);
    return new TypeSafeMatcher<Iterable<? extends T>>() {

      @Override
      protected boolean matchesSafely(Iterable<? extends T> ts) {
        int ai = -1, bi = -1, i = 0;
        for (T t : ts) {
          if (a.equals(t)) {
            ai = i;
          }
          if (b.equals(t)) {
            bi = i;
          }
          i++;
        }

        return ai > -1 && bi > -1 && ai < bi;
      }

      @Override
      public void describeTo(Description description) {
        description.appendText("Contains ");
        description.appendValue(a);
        description.appendText(" before ");
        description.appendValue(b);
      }
    };
  }
}
