package com.spotify.flo;

import static java.util.Arrays.asList;

import io.vavr.Tuple;
import io.vavr.Tuple2;
import io.vavr.Tuple3;
import io.vavr.Tuple4;
import io.vavr.Tuple5;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

public class MultiTaskOutput {

  private MultiTaskOutput() {
    throw new UnsupportedOperationException();
  }

  @SuppressWarnings("unchecked")
  public static <T1, T2, S1, S2> TaskOutput<Tuple2<T1, T2>, Tuple2<S1, S2>> of(
      TaskOutput<T1, S1> o1,
      TaskOutput<T2, S2> o2) {
    return new Output<>(o1, o2);
  }

  @SuppressWarnings("unchecked")
  public static <T1, T2, T3, S1, S2, S3> TaskOutput<Tuple3<T1, T2, T3>, Tuple3<S1, S2, S3>> of(
      TaskOutput<T1, S1> o1,
      TaskOutput<T2, S2> o2,
      TaskOutput<T3, S3> o3) {
    return new Output<>(o1, o2, o3);
  }

  @SuppressWarnings("unchecked")
  public static <T1, T2, T3, T4, S1, S2, S3, S4> TaskOutput<Tuple4<T1, T2, T3, T4>, Tuple4<S1, S2, S3, S4>> of(
      TaskOutput<T1, S1> o1,
      TaskOutput<T2, S2> o2,
      TaskOutput<T3, S3> o3,
      TaskOutput<T3, S3> o4) {
    return new Output<>(o1, o2, o3, o4);
  }

  @SuppressWarnings("unchecked")
  public static <T1, T2, T3, T4, T5, S1, S2, S3, S4, S5> TaskOutput<Tuple5<T1, T2, T3, T4, T5>, Tuple5<S1, S2, S3, S4, S5>> of(
      TaskOutput<T1, S1> o1,
      TaskOutput<T2, S2> o2,
      TaskOutput<T3, S3> o3,
      TaskOutput<T3, S3> o4,
      TaskOutput<T3, S3> o5) {
    return new Output<>(o1, o2, o3, o4, o5);
  }

  public static class Output<S extends Tuple> extends TaskOutput {

    private final List<TaskOutput> outputs;

    private Output(TaskOutput... outputs) {
      this.outputs = Objects.requireNonNull(asList(outputs));
    }

    @SuppressWarnings("unchecked")
    @Override
    public Optional<S> lookup(TaskId taskId) {
      final List<Optional> lookups = outputs.stream()
          .map(output -> output.lookup(taskId))
          .collect(Collectors.toList());
      if (lookups.stream().anyMatch(o -> !o.isPresent())) {
        return Optional.empty();
      } else {
        return (Optional<S>) Optional.of(tuple(lookups.stream().map(Optional::get).toArray()));
      }
    }

    @Override
    public Object provide(EvalContext evalContext) {
      return tuple(outputs.stream().map(output -> output.provide(evalContext)).toArray());
    }

    @SuppressWarnings("unchecked")
    @Override
    public void preRun(Task task) {
      for (TaskOutput output : outputs) {
        output.preRun(task);
      }
    }

    @Override
    public void onSuccess(Task task, Object z) {
      for (TaskOutput output : outputs) {
        output.onSuccess(task, z);
      }
    }

    @Override
    public void onFail(Task task, Throwable throwable) {
      for (TaskOutput output : outputs) {
        output.onFail(task, throwable);
      }
    }
  }

  private static Tuple tuple(Object[] o) {
    switch (o.length) {
      case 0:
      case 1:
        throw new IllegalArgumentException();
      case 2:
        return Tuple.of(o[0], o[1]);
      case 3:
        return Tuple.of(o[0], o[1], o[2]);
      case 4:
        return Tuple.of(o[0], o[1], o[2], o[3]);
      case 5:
        return Tuple.of(o[0], o[1], o[2], o[3], o[4]);
      default:
        throw new UnsupportedOperationException();
    }
  }
}
