package io.rouz.flo;

import static org.junit.Assert.assertTrue;

import io.rouz.flo.context.AwaitingConsumer;

/**
 * Testing utility for waiting for values
 */
public final class AwaitValue<T> extends AwaitingConsumer<T> {

  private String acceptingThreadName;

  @Override
  public void accept(T t) {
    acceptingThreadName = Thread.currentThread().getName();
    super.accept(t);
  }

  @Override
  public T awaitAndGet() throws InterruptedException {
    assertTrue("wait for value", await());
    return super.get();
  }

  public String acceptingThreadName() {
    return acceptingThreadName;
  }
}
