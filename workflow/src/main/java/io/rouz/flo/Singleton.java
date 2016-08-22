package io.rouz.flo;

import java.util.Objects;

/**
 * A singleton supplier decorator.
 *
 * Ensures that the {@link #get()} method of the wrapped {@link Fn} only is called once.
 */
class Singleton<T> implements Fn<T> {

  private final Fn<T> supplier;
  private volatile T value;

  private Singleton(Fn<T> supplier) {
    this.supplier = Objects.requireNonNull(supplier);
  }

  static <T> Fn<T> create(Fn<T> fn) {
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
