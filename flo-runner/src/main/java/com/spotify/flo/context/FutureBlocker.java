/*-
 * -\-\-
 * flo runner
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

import com.google.common.base.Throwables;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A utility class for waiting on a set of registered futures to complete.
 *
 * <p>It implements {@link Closeable} with a {@link #close()} method that blocks until all
 * {@link #register(ListenableFuture)}-ed futures have completed.
 */
class FutureBlocker implements Closeable {

  private AtomicInteger pendingFutures = new AtomicInteger(0);

  @Override
  public void close() throws IOException {
    while (pendingFutures.get() != 0) {
      try {
        Thread.sleep(10);
      } catch (InterruptedException e) {
        throw Throwables.propagate(e);
      }
    }
  }

  void register(ListenableFuture<?> future) {
    pendingFutures.incrementAndGet();
    future.addListener(() -> pendingFutures.decrementAndGet(), MoreExecutors.directExecutor());
  }
}
