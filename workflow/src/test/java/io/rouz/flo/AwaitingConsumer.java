package io.rouz.flo;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static org.junit.Assert.assertTrue;

/**
 * Testing utility for waiting for values
 */
public final class AwaitingConsumer<T> implements Consumer<T> {

  private final CountDownLatch latch = new CountDownLatch(1);
  private T value;
  private String acceptingThreadName;

  @Override
  public void accept(T t) {
    acceptingThreadName = Thread.currentThread().getName();
    value = t;
    latch.countDown();
  }

  public boolean isAvailable() {
    return latch.getCount() == 0;
  }

  public boolean await() throws InterruptedException {
    return await(1, TimeUnit.SECONDS);
  }

  public boolean await(long time, TimeUnit unit) throws InterruptedException {
    return latch.await(time, unit);
  }

  public T awaitAndGet() throws InterruptedException {
    assertTrue("wait for value", await());
    return value;
  }

  public String acceptingThreadName() {
    return acceptingThreadName;
  }
}
