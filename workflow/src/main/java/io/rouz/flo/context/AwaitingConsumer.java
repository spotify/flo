package io.rouz.flo.context;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * A {@link Consumer} that allows for synchronously waiting for the value to arrive.
 */
public class AwaitingConsumer<T> implements Consumer<T> {

  private final CountDownLatch latch = new CountDownLatch(1);
  private T value;

  public static <T> AwaitingConsumer<T> create() {
    return new AwaitingConsumer<>();
  }

  @Override
  public void accept(T t) {
    value = t;
    latch.countDown();
  }

  public T get() {
    if (!isAvailable()) {
      throw new IllegalStateException("Value not available");
    }

    return value;
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
    await();
    return value;
  }
}
