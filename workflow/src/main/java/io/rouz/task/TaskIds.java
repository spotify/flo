package io.rouz.task;

import com.google.auto.value.AutoValue;

import java.io.Serializable;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * {@link AutoValue} implementation of {@link TaskId}
 */
@AutoValue
abstract class TaskIds implements TaskId, Serializable {

  abstract String args();

  static TaskId create(String name, Object... args) {
    if (name.contains("(") || name.contains(")")) {
      throw new IllegalArgumentException("Name can not contain any parenthesis");
    }

    return new AutoValue_TaskIds(
        name,
        name.hashCode() * 1000003 ^ Objects.hash(args),
        argsString(args));
  }

  static TaskId parse(String stringId) {
    final int pOpen = stringId.indexOf('(');
    final int pClose = stringId.lastIndexOf(')');
    final int pHash = stringId.lastIndexOf('#');

    if (pOpen < 0 || pClose < pOpen || pHash < pClose) {
      throw new IllegalArgumentException("Invalid stringId, follow 'Name(foo,bar)#deadbeef'");
    }

    final String name = stringId.substring(0, pOpen);
    final String args = stringId.substring(pOpen + 1, pClose);
    final String hash = stringId.substring(pHash + 1);

    return new AutoValue_TaskIds(
        name,
        Integer.parseUnsignedInt(hash, 16),
        args);
  }

  @Override
  public String toString() {
    return String.format("%s(%s)#%08x", name(), args(), hash());
  }

  private static String argsString(Object... args) {
    return Stream.of(args)
        .map(Object::toString)
        .collect(Collectors.joining(","));
  }
}
