package io.rouz.task;

import java.util.Objects;

import io.rouz.task.TaskBuilder.F0;

/**
 * A singleton supplier decorator.
 *
 * Ensures that the {@link #get()} method of the wrapped {@link F0} only is called once.
 */
class Singleton<T> implements F0<T> {

  private final F0<T> supplier;
  private volatile T value;

  private Singleton(F0<T> supplier) {
    this.supplier = Objects.requireNonNull(supplier);
  }

  static <T> F0<T> create(F0<T> fn) {
    return new Singleton<>(fn);
  }

  @Override
  public T get() {
    if (value == null) {
      synchronized (this) {
        if (value == null) {
          value = supplier.get();
        }
      }
    }
    return value;
  }
}
