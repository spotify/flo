package io.rouz.task.proc;

import com.google.auto.value.AutoValue;

import java.util.Arrays;

import io.rouz.task.TaskBuilder.F1;
import io.rouz.task.TaskBuilder.F2;
import io.rouz.task.TaskBuilder.F3;

/**
 * TODO: document.
 */
public final class Exec {

  private Exec() {
    // no instantiation
  }

  public static <A> F1<A, Result> exec(F1<A, String[]> f) {
    return a -> {
      final String[] args = f.apply(a);
      System.out.println("running " + Arrays.toString(args));
      // exec(args);
      return new AutoValue_Exec_ResultValue(args.length);
    };
  }

  public static <A, B> F2<A, B, Result> exec(F2<A, B, String[]> f) {
    return (a, b) -> {
      final String[] args = f.apply(a, b);
      System.out.println("running " + Arrays.toString(args));
      // exec(args);
      return new AutoValue_Exec_ResultValue(args.length);
    };
  }

  public static <A, B, C> F3<A, B, C, Result> exec(F3<A, B, C, String[]> f) {
    return (a, b, c) -> {
      final String[] args = f.apply(a, b, c);
      System.out.println("running " + Arrays.toString(args));
      // exec(args);
      return new AutoValue_Exec_ResultValue(args.length);
    };
  }

  public interface Result {
    int exitCode();
  }

  @AutoValue
  static abstract class ResultValue implements Result {
  }
}
