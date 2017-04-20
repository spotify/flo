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

  @Override
  public void accept(T t) {
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
    return value;
  }
}
