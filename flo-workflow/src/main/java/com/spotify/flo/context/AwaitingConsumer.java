/*-
 * -\-\-
 * Flo Workflow Definition
 * --
 * Copyright (C) 2016 - 2017 Spotify AB
 * --
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * -/-/-
 */

package com.spotify.flo.context;

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
    if (!await()) {
      throw new IllegalStateException("Value did not arrive");
    }
    return value;
  }
}
