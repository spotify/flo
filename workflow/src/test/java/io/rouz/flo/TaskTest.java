package io.rouz.flo;

import org.junit.Test;

import java.util.concurrent.atomic.AtomicReference;

import io.rouz.flo.TaskContext.Promise;

import static java.util.Arrays.asList;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * Naming convention for tests
 * XXX_X
 * ^^^ ^
 * ||| `--> N,R = normal, curried
 * ||`----> D,C = direct, context
 * |`-----> I,L = in, ins
 * `------> arity
 *
 * eg 2RD_IL = arity 2 curried direct processed task with single input and list input
 */
public class TaskTest {

  // 0. ===========================================================================================

  @Test
  public void shouldEvaluate0ND() throws Exception {
    Task<String> task = Task.named("InContext").ofType(String.class)
        .process(() -> "constant");

    AwaitingConsumer<String> val = new AwaitingConsumer<>();
    ControlledBlockingContext context = new ControlledBlockingContext();
    context.evaluate(task).consume(val);

    context.waitFor(task);
    context.release(task);
    context.waitUntilNumConcurrent(0);
    assertThat(val.awaitAndGet(), is("constant"));
  }

  @Test
  public void shouldEvaluate0NC() throws Exception {
    AtomicReference<Promise<String>> promiseRef = new AtomicReference<>();
    Task<String> task = Task.named("InContext").ofType(String.class).processWithContext(tc -> {
      Promise<String> promise = tc.promise();
      promiseRef.set(promise);
      return promise.value();
    });

    validatePromiseEvaluation(task, promiseRef, "");
  }

  // 1. ===========================================================================================

  @Test
  public void shouldEvaluate1ND_I() throws Exception {
    Task<String> task = Task.named("InContext").ofType(String.class)
        .in(() -> leaf("A"))
        .process((a) -> "done: " + a);

    validateEvaluation(task, "done: A", leaf("A"));
  }

  @Test
  public void shouldEvaluate1ND_L() throws Exception {
    Task<String> task = Task.named("InContext").ofType(String.class)
        .ins(() -> asList(leaf("A"), leaf("B")))
        .process((ab) -> "done: " + ab);

    validateEvaluation(task, "done: [A, B]", leaf("A"), leaf("B"));
  }

  @Test
  public void shouldEvaluate1NC_I() throws Exception {
    AtomicReference<Promise<String>> promiseRef = new AtomicReference<>();
    Task<String> task = Task.named("InContext").ofType(String.class)
        .in(() -> leaf("A"))
        .processWithContext((tc, a) -> {
          Promise<String> promise = tc.promise();
          promiseRef.set(promise);
          return promise.value().map(v -> v + " - " + a);
        });

    validatePromiseEvaluation(task, promiseRef, " - A", leaf("A"));
  }

  @Test
  public void shouldEvaluate1NC_L() throws Exception {
    AtomicReference<Promise<String>> promiseRef = new AtomicReference<>();
    Task<String> task = Task.named("InContext").ofType(String.class)
        .ins(() -> asList(leaf("A"), leaf("B")))
        .processWithContext((tc, ab) -> {
          Promise<String> promise = tc.promise();
          promiseRef.set(promise);
          return promise.value().map(v -> v + " - " + ab);
        });

    validatePromiseEvaluation(task, promiseRef, " - [A, B]", leaf("A"), leaf("B"));
  }

  // 2. ===========================================================================================

  @Test
  public void shouldEvaluate2ND_II() throws Exception {
    Task<String> task = Task.named("InContext").ofType(String.class)
        .in(() -> leaf("A"))
        .in(() -> leaf("B"))
        .process((a, b) -> "done: " + a + " - " + b);

    validateEvaluation(task, "done: A - B", leaf("A"), leaf("B"));
  }

  @Test
  public void shouldEvaluate2ND_IL() throws Exception {
    Task<String> task = Task.named("InContext").ofType(String.class)
        .in(() -> leaf("A"))
        .ins(() -> asList(leaf("B"), leaf("C")))
        .process((a, bc) -> "done: " + a + " - " + bc);

    validateEvaluation(task, "done: A - [B, C]", leaf("A"), leaf("B"), leaf("C"));
  }

  @Test
  public void shouldEvaluate2NC_II() throws Exception {
    AtomicReference<Promise<String>> promiseRef = new AtomicReference<>();
    Task<String> task = Task.named("InContext").ofType(String.class)
        .in(() -> leaf("A"))
        .in(() -> leaf("B"))
        .processWithContext((tc, a, b) -> {
          Promise<String> promise = tc.promise();
          promiseRef.set(promise);
          return promise.value().map(v -> v + " - " + a + " - " + b);
        });

    validatePromiseEvaluation(task, promiseRef, " - A - B", leaf("A"), leaf("B"));
  }

  @Test
  public void shouldEvaluate2NC_IL() throws Exception {
    AtomicReference<Promise<String>> promiseRef = new AtomicReference<>();
    Task<String> task = Task.named("InContext").ofType(String.class)
        .in(() -> leaf("A"))
        .ins(() -> asList(leaf("B"), leaf("C")))
        .processWithContext((tc, a, bc) -> {
          Promise<String> promise = tc.promise();
          promiseRef.set(promise);
          return promise.value().map(v -> v + " - " + a + " - " + bc);
        });

    validatePromiseEvaluation(task, promiseRef, " - A - [B, C]", leaf("A"), leaf("B"), leaf("C"));
  }

  // 3. ===========================================================================================

  @Test
  public void shouldEvaluate3ND_III() throws Exception {
    Task<String> task = Task.named("InContext").ofType(String.class)
        .in(() -> leaf("A"))
        .in(() -> leaf("B"))
        .in(() -> leaf("C"))
        .process((a, b, c) -> "done: " + a + " - " + b +" - " + c);

    validateEvaluation(task, "done: A - B - C", leaf("A"), leaf("B"), leaf("C"));
  }

  @Test
  public void shouldEvaluate3ND_IIL() throws Exception {
    Task<String> task = Task.named("InContext").ofType(String.class)
        .in(() -> leaf("A"))
        .in(() -> leaf("B"))
        .ins(() -> asList(leaf("C"), leaf("D")))
        .process((a, b, cd) -> "done: " + a + " - " + b +" - " + cd);

    validateEvaluation(task, "done: A - B - [C, D]", leaf("A"), leaf("B"), leaf("C"), leaf("D"));
  }

  @Test
  public void shouldEvaluate3NC_III() throws Exception {
    AtomicReference<Promise<String>> promiseRef = new AtomicReference<>();
    Task<String> task = Task.named("InContext").ofType(String.class)
        .in(() -> leaf("A"))
        .in(() -> leaf("B"))
        .in(() -> leaf("C"))
        .processWithContext((tc, a, b, c) -> {
          Promise<String> promise = tc.promise();
          promiseRef.set(promise);
          return promise.value().map(v -> v + " - " + a + " - " + b +" - " + c);
        });

    validatePromiseEvaluation(task, promiseRef, " - A - B - C", leaf("A"), leaf("B"), leaf("C"));
  }

  @Test
  public void shouldEvaluate3NC_IIL() throws Exception {
    AtomicReference<Promise<String>> promiseRef = new AtomicReference<>();
    Task<String> task = Task.named("InContext").ofType(String.class)
        .in(() -> leaf("A"))
        .in(() -> leaf("B"))
        .ins(() -> asList(leaf("C"), leaf("D")))
        .processWithContext((tc, a, b, cd) -> {
          Promise<String> promise = tc.promise();
          promiseRef.set(promise);
          return promise.value().map(v -> v + " - " + a + " - " + b +" - " + cd);
        });

    validatePromiseEvaluation(task, promiseRef, " - A - B - [C, D]", leaf("A"), leaf("B"),
                              leaf("C"), leaf("D"));
  }

  // Curried ======================================================================================

  @Test
  public void shouldEvaluate1RN_I() throws Exception {
    Task<String> task = Task.named("InContext").ofType(String.class).curried()
        .in(() -> leaf("A"))
        .process(a -> "done: " + a);

    validateEvaluation(task, "done: A", leaf("A"));
  }

  @Test
  public void shouldEvaluate1RN_L() throws Exception {
    Task<String> task = Task.named("InContext").ofType(String.class).curried()
        .ins(() -> asList(leaf("A"), leaf("B")))
        .process(ab -> "done: " + ab);

    validateEvaluation(task, "done: [A, B]", leaf("A"), leaf("B"));
  }

  @Test
  public void shouldEvaluate1RC_I() throws Exception {
    AtomicReference<Promise<String>> promiseRef = new AtomicReference<>();
    Task<String> task = Task.named("InContext").ofType(String.class).curriedWithContext()
        .in(() -> leaf("A"))
        .process(tc -> a -> {
          Promise<String> promise = tc.promise();
          promiseRef.set(promise);
          return promise.value().map(v -> v + " - " + a);
        });

    validatePromiseEvaluation(task, promiseRef, " - A", leaf("A"));
  }

  @Test
  public void shouldEvaluate1RC_L() throws Exception {
    AtomicReference<Promise<String>> promiseRef = new AtomicReference<>();
    Task<String> task = Task.named("InContext").ofType(String.class).curriedWithContext()
        .ins(() -> asList(leaf("A"), leaf("B")))
        .process(tc -> ab -> {
          Promise<String> promise = tc.promise();
          promiseRef.set(promise);
          return promise.value().map(v -> v + " - " + ab);
        });

    validatePromiseEvaluation(task, promiseRef, " - [A, B]", leaf("A"), leaf("B"));
  }

  // 2. (higher arities tested inductively)

  @Test
  public void shouldEvaluate2RN_II() throws Exception {
    Task<String> task = Task.named("InContext").ofType(String.class).curried()
        .in(() -> leaf("A"))
        .in(() -> leaf("B"))
        .process(b -> a -> "done: " + a  + " - " + b);

    validateEvaluation(task, "done: A - B", leaf("A"), leaf("B"));
  }

  @Test
  public void shouldEvaluate2RN_IL() throws Exception {
    Task<String> task = Task.named("InContext").ofType(String.class).curried()
        .in(() -> leaf("A"))
        .ins(() -> asList(leaf("B"), leaf("C")))
        .process(bc -> a -> "done: " + a + " - " + bc);

    validateEvaluation(task, "done: A - [B, C]", leaf("A"), leaf("B"), leaf("C"));
  }

  @Test
  public void shouldEvaluate2RC_II() throws Exception {
    AtomicReference<Promise<String>> promiseRef = new AtomicReference<>();
    Task<String> task = Task.named("InContext").ofType(String.class).curriedWithContext()
        .in(() -> leaf("A"))
        .in(() -> leaf("B"))
        .process(tc -> b -> a -> {
          Promise<String> promise = tc.promise();
          promiseRef.set(promise);
          return promise.value().map(v -> v + " - " + a + " - " + b);
        });

    validatePromiseEvaluation(task, promiseRef, " - A - B", leaf("A"), leaf("B"));
  }

  @Test
  public void shouldEvaluate2RC_IL() throws Exception {
    AtomicReference<Promise<String>> promiseRef = new AtomicReference<>();
    Task<String> task = Task.named("InContext").ofType(String.class).curriedWithContext()
        .in(() -> leaf("A"))
        .ins(() -> asList(leaf("B"), leaf("C")))
        .process(tc -> bc -> a -> {
          Promise<String> promise = tc.promise();
          promiseRef.set(promise);
          return promise.value().map(v -> v + " - " + a + " - " + bc);
        });

    validatePromiseEvaluation(task, promiseRef, " - A - [B, C]", leaf("A"), leaf("B"), leaf("C"));
  }

  // ==============================================================================================

  @Test
  public void shouldHaveClassOfTaskType() throws Exception {
    Task<String> task0 = Task.named("WithType").ofType(String.class)
        .process(() -> "");
    Task<String> task1 = Task.named("WithType").ofType(String.class)
        .in(() -> leaf("A"))
        .process((a) -> a);
    Task<String> task1c = Task.named("WithType").ofType(String.class).curried()
        .in(() -> leaf("A"))
        .process(a -> a);
    Task<String> task1cc = Task.named("WithType").ofType(String.class).curriedWithContext()
        .in(() -> leaf("A"))
        .process(tc -> tc::immediateValue);
    Task<String> task2 = Task.named("WithType").ofType(String.class)
        .in(() -> leaf("A"))
        .in(() -> leaf("B"))
        .process((a, b) -> a + " - " + b);
    Task<String> task2c = Task.named("WithType").ofType(String.class).curried()
        .in(() -> leaf("A"))
        .in(() -> leaf("B"))
        .process(a -> b -> a + " - " + b);
    Task<String> task2cc = Task.named("WithType").ofType(String.class).curriedWithContext()
        .in(() -> leaf("A"))
        .in(() -> leaf("B"))
        .process(tc -> a -> b -> tc.immediateValue(a + " - " + b));
    Task<String> task3 = Task.named("WithType").ofType(String.class)
        .in(() -> leaf("A"))
        .in(() -> leaf("B"))
        .in(() -> leaf("C"))
        .process((a, b, c) -> a + " - " + b +" - " + c);
    Task<String> task3c = Task.named("WithType").ofType(String.class).curried()
        .in(() -> leaf("A"))
        .in(() -> leaf("B"))
        .in(() -> leaf("C"))
        .process(a -> b -> c -> a + " - " + b +" - " + c);
    Task<String> task3cc = Task.named("WithType").ofType(String.class).curriedWithContext()
        .in(() -> leaf("A"))
        .in(() -> leaf("B"))
        .in(() -> leaf("C"))
        .process(tc -> a -> b -> c -> tc.immediateValue(a + " - " + b +" - " + c));

    assertThat(task0.type(), equalTo(String.class));
    assertThat(task1.type(), equalTo(String.class));
    assertThat(task1c.type(), equalTo(String.class));
    assertThat(task1cc.type(), equalTo(String.class));
    assertThat(task2.type(), equalTo(String.class));
    assertThat(task2c.type(), equalTo(String.class));
    assertThat(task2cc.type(), equalTo(String.class));
    assertThat(task3.type(), equalTo(String.class));
    assertThat(task3c.type(), equalTo(String.class));
    assertThat(task3cc.type(), equalTo(String.class));
  }

  // Validators ===================================================================================

  private void validateEvaluation(
      Task<String> task,
      String expectedOutput,
      Task... inputs)
      throws InterruptedException {

    AwaitingConsumer<String> val = new AwaitingConsumer<>();
    ControlledBlockingContext context = new ControlledBlockingContext();
    context.evaluate(task).consume(val);

    context.waitFor(task);
    context.release(task);
    context.waitUntilNumConcurrent(inputs.length + 1); // task + inputs
    for (Task input : inputs) {
      assertTrue(context.isWaiting(input));
    }
    assertFalse(val.isAvailable());

    for (Task input : inputs) {
      context.release(input);
    }
    context.waitUntilNumConcurrent(0);
    assertThat(val.awaitAndGet(), is(expectedOutput));
  }

  private void validatePromiseEvaluation(
      Task<String> task,
      AtomicReference<Promise<String>> promiseRef,
      String expectedOutput,
      Task... inputs)
      throws InterruptedException {

    AwaitingConsumer<String> val = new AwaitingConsumer<>();
    ControlledBlockingContext context = new ControlledBlockingContext();
    context.evaluate(task).consume(val);

    context.waitFor(task);
    context.release(task);
    context.waitUntilNumConcurrent(inputs.length + 1); // task + inputs
    for (Task input : inputs) {
      assertTrue(context.isWaiting(input));
    }
    assertFalse(val.isAvailable());

    for (Task input : inputs) {
      context.release(input);
    }
    context.waitUntilNumConcurrent(1); // task will not complete, promise still waiting
    assertFalse(val.isAvailable());

    //noinspection StatementWithEmptyBody
    while (promiseRef.get() == null) {
    }

    promiseRef.get().set("done: from here");
    context.waitUntilNumConcurrent(0);
    assertThat(val.awaitAndGet(), is("done: from here" + expectedOutput));
  }

  Task<String> leaf(String s) {
    return Task.named("Leaf", s).ofType(String.class).process(() -> s);
  }
}
