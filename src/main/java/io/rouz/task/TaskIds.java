package io.rouz.task;

import com.google.auto.value.AutoValue;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * {@link AutoValue} implementation of {@link TaskId}
 */
@AutoValue
abstract class TaskIds implements TaskId {

  abstract List<Object> args();

  static TaskId create(String name, Object... args) {
    return new AutoValue_TaskIds(name, Objects.hash(args), Arrays.asList(args));
  }

  @Override
  public String toString() {
    return name() + argsString() + String.format("#%08x", hash());
  }

  private String argsString() {
    final StringBuilder sb = new StringBuilder("(");
    boolean first = true;
    for (Object arg : args()) {
      if (!first) {
        sb.append(',');
      }
      sb.append(arg);
      first = false;
    }
    sb.append(')');

    return sb.toString();
  }
}
