package io.rouz.task;

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import io.rouz.task.dsl.TaskBuilder;
import io.rouz.task.dsl.TaskBuilder.F0;
import io.rouz.task.dsl.TaskBuilder.F1;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class TaskTest {

  private final List<String> tasks = new ArrayList<>();

  @Test
  public void shouldRunAsExpected() throws Exception {
    Task<EvenResult> wasEven = isEven(6);
    Task<EvenResult> madeEven = isEven(5);

    assertThat(wasEven.out(), instanceOf(WasEven.class));
    assertThat(wasEven.out().result(), is(6));
    assertThat(madeEven.out(), instanceOf(MadeEven.class));
    assertThat(madeEven.out().result(), is(10));
  }

  @Test
  public void shouldMemoizeTaskProcessing() throws Exception {
    AtomicInteger counter = new AtomicInteger(0);
    Task<Integer> count = Task.named("Count")
        .constant(counter::incrementAndGet);

    Task<Integer> sum = Task.named("Sum")
        .in(() -> count)
        .in(() -> count)
        .in(() -> count)
        .process((a, b, c) -> a + b + c);

    assertThat(sum.out(), is(3));
    assertThat(counter.get(), is(1)); // only called once

    // only memoized during each execution
    assertThat(count.out(), is(2));
    assertThat(count.out(), is(3));
    assertThat(counter.get(), is(3)); // called twice more
  }

  @Test
  public void shouldHanleStreamParameters() throws Exception {
    F0<Task<Integer>> countSupplier = countConstructor();

    // 1,2,3,4,5
    List<Task<Integer>> fiveInts = Stream
        .generate(countSupplier)
        .limit(5)
        .collect(toList());

    Task<Integer> sum = Task.named("Sum")
        .ins(() -> fiveInts)
        .process(this::sumInts);

    // 1+2+3+4+5 = 15
    assertThat(sum.out(), is(15));
  }

  @Test
  public void shouldHanleMixedStreamAndPlainParameters() throws Exception {
    F0<Task<Integer>> countSupplier = countConstructor();

    // 1,2,3,4,5
    List<Task<Integer>> fiveInts = Stream
        .generate(countSupplier)
        .limit(5)
        .collect(toList());

    Task<Integer> sum = Task.named("Sum")
        .in(() -> isEven(5))
        .ins(() -> fiveInts)
        .in(() -> isEven(2))
        .process((a, ints, b) -> a.result() + sumInts(ints) + b.result());

    // (5*2) + (1+2+3+4+5) + 2 = 27
    assertThat(sum.out(), is(27));
  }

  @Test
  public void shouldHanleMultipleStreamParameters() throws Exception {
    F0<Task<Integer>> countSupplier = countConstructor();

    F0<List<Task<Integer>>> fiveInts = () -> Stream
        .generate(countSupplier)
        .limit(5)
        .collect(toList());

    Task<Integer> sum = Task.named("Sum")
        .ins(fiveInts)
        .ins(fiveInts)
        .process((first5, second5) -> sumInts(first5) + sumInts(second5));

    // (1+2+3+4+5) + (6+7+8+9+10) = 55
    assertThat(sum.out(), is(55));
  }

  @Test
  public void shoulAllowMultipleRunsWithStreamParameters() throws Exception {
    F0<Task<Integer>> countSupplier = countConstructor();

    F0<List<Task<Integer>>> fiveInts = () -> Stream
        .generate(countSupplier)
        .limit(5)
        .collect(toList());

    Task<Integer> sum = Task.named("Sum")
        .ins(fiveInts)
        .process(this::sumInts);

    // discard first five
    sum.out();

    // 6+7+8+9+10 = 40
    assertThat(sum.out(), is(40));
  }

  @Test
  public void shouldMultipleRunsWithMultipleStreamParameters() throws Exception {
    F0<Task<Integer>> countSupplier = countConstructor();

    F0<List<Task<Integer>>> fiveInts = () -> Stream
        .generate(countSupplier)
        .limit(5)
        .collect(toList());

    Task<Integer> sum = Task.named("Sum")
        .ins(fiveInts)
        .ins(fiveInts)
        .process((first5, second5) -> sumInts(first5) + sumInts(second5));

    // discard first 2 groups of five
    sum.out();

    // (11+12+13+14+15) + (16+17+18+19+20) = 155
    assertThat(sum.out(), is(155));
  }

  @Test
  public void shouldLinearizeTasks() throws Exception {
    Task<String> top = Task.named("Top")
        .in(() -> isEven(0))
        .in(() -> isEven(1))
        .process((a, b) -> "done");

    top.out();

    String madeEven2 = "MadeEven 2";
    String evenify1 = "Evenify 1";

    assertThat(tasks, containsInOrder(evenify1, madeEven2));
  }

  @Test
  public void shouldFlattenStreamParameters() throws Exception {
    Task<String> top = Task.named("Top")
        .ins(() -> asList(isEven(0), isEven(1)))
        .process(results -> "done " + results.size());

    top.out();

    String evenify1 = "Evenify 1";
    String madeEven2 = "MadeEven 2";
    String wasEven0 = "WasEven 0";

    assertThat(tasks.size(), is(3));
    assertThat(tasks, containsInOrder(wasEven0, evenify1));
    assertThat(tasks, containsInOrder(wasEven0, madeEven2));
    assertThat(tasks, containsInOrder(evenify1, madeEven2));
  }

  @Test
  public void shouldLinearizeMixedStreamAndPlainParameters() throws Exception {
    F1<Integer, Task<Integer>> evenResult = n ->
        Task.named("EvenResult", n)
            .in(() -> isEven(n))
            .process(EvenResult::result);

    Task<Integer> sum = Task.named("Sum")
        .in(() -> isEven(5))
        .ins(() -> asList(evenResult.apply(0), evenResult.apply(1)))
        .ins(() -> singletonList(evenResult.apply(3)))
        .process((a, ints, b) -> a.result() + sumInts(ints) + sumInts(b));

    sum.out();

    String evenify5 = "Evenify 5";
    String evenify1 = "Evenify 1";
    String evenify3 = "Evenify 3";

    assertThat(tasks.size(), is(7));
    assertThat(tasks, containsInOrder(evenify5, evenify1));
    assertThat(tasks, containsInOrder(evenify5, evenify3));
    assertThat(tasks, containsInOrder(evenify1, evenify3));
  }

  @Test
  public void shouldBuildArbitraryDeepCurriedLambda() throws Exception {
    final Task<Integer> curried = Task.named("Curried")
        .curryTo(Integer.class)
        .in(() -> isEven(0)) // 0
        .in(() -> isEven(1)) // 2
        .in(() -> isEven(2)) // 2
        .in(() -> isEven(3)) // 6
        .in(() -> isEven(4)) // 4
        .in(() -> isEven(5)) // 10
        .process(
            a -> b -> c -> d -> e -> f ->
                a.result +
                b.result +
                c.result +
                d.result +
                e.result +
                f.result
        );

    assertThat(curried.out(), is(24));
  }

  @Test
  public void shouldBuildCurriedLambdaWithLists() throws Exception {
    final Task<Integer> curried = Task.named("Curried")
        .curryTo(Integer.class)
        .ins(() -> asList(isEven(11), isEven(20))) // [22, 20]
        .in(() -> isEven(0)) // 0
        .ins(() -> asList(isEven(1), isEven(2))) // [2, 2]
        .in(() -> isEven(5)) // 10
        .process(
            a -> b -> c -> d ->
                a.result +
                b.stream().mapToInt(EvenResult::result).sum() +
                c.result +
                d.stream().mapToInt(EvenResult::result).sum()
        );

    assertThat(curried.out(), is(56));
  }

  private F0<Task<Integer>> countConstructor() {
    AtomicInteger counter = new AtomicInteger(0);
    return () -> {
      int n = counter.incrementAndGet();
      return Task.named("Count", n)
          .constant(() -> n);
    };
  }

  private int sumInts(List<Integer> intsList) {
    return intsList.stream().reduce(0, (a, b) -> a + b);
  }

  private Task<EvenResult> isEven(int n) {
    TaskBuilder isEven = Task.named("IsEven", n);

    if (n % 2 == 0) {
      return isEven.constant(() -> new WasEven(n));
    }

    return isEven
        .in(() -> evenify(n))
        .process(MadeEven::new);
  }

  private Task<Integer> evenify(int n) {
    return Task.named("Evenify", n)
        .constant(() -> {
          tasks.add("Evenify " + n);
          return n * 2;
        });
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
      tasks.add("WasEven "  + result);
    }
  }

  class MadeEven extends EvenResult {

    MadeEven(int result) {
      super(result);
      tasks.add("MadeEven "  + result);
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
