package io.rouz.flo;

import io.rouz.flo.TaskContext.Value;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collector;
import java.util.stream.Stream;

/**
 * A function that folds a {@link List} of {@link Value}s into a {@link Value} of a {@link List}.
 *
 * <p>It can be used with a {@link Collector} to fold a {@link Stream} of {@link Value}s.
 */
final class ValueFold<T> implements Function<List<Value<T>>, Value<List<T>>> {

  private final TaskContext taskContext;

  ValueFold(TaskContext taskContext) {
    this.taskContext = Objects.requireNonNull(taskContext);
  }

  static <T> ValueFold<T> inContext(TaskContext taskContext) {
    return new ValueFold<>(taskContext);
  }

  @Override
  public Value<List<T>> apply(List<Value<T>> list) {
    Value<List<T>> values = taskContext.immediateValue(new ArrayList<>());
    for (Value<T> tValue : list) {
      values = Values.mapBoth(taskContext, values, tValue, (l, t) -> {
        l.add(t);
        return l;
      });
    }
    return values;
  }
}
