/*-
 * -\-\-
 * Flo Runner
 * --
 * Copyright (C) 2016 - 2018 Spotify AB
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

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.collection.IsIterableContainingInOrder.contains;
import static org.junit.Assert.assertThat;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;
import org.junit.Test;

public class ThreadAwarePrintStreamTest {

  @Test
  public void testThreadAwarePrintStream() {
    AtomicLong threads = new AtomicLong(0);

    ThreadFactory threadFactory = r -> {
      Thread thread = new Thread(r);
      thread.setName(String.format("foo-worker-%d", threads.getAndIncrement()));
      return thread;
    };

    ExecutorService pool = Executors.newFixedThreadPool(3, threadFactory);

    Collection<String> out = new ArrayList<>();
    Collection<String> err = new ArrayList<>();

    PrintStream originalOut = System.out;
    PrintStream originalErr = System.err;

    try (ThreadAwarePrintStream.Overrider ignore =
             ThreadAwarePrintStream.override("foo-worker-", out::add, err::add)) {

      assertThat(System.out, is(not(originalOut)));
      assertThat(System.err, is(not(originalErr)));

      IntStream.range(0, 5)
          .forEach(i ->
              CompletableFuture.runAsync(
                  () -> {
                    System.out.println("out" + i);
                    System.err.println("err" + i);
                  }, pool)
                  .join()
          );

      System.out.println("stdout inside try-resources is not collected");
      System.err.println("stderr inside try-resources is not collected");
    }

    assertThat(System.out, is(originalOut));
    assertThat(System.err, is(originalErr));

    System.out.println("stdout outside try-resources is not collected");
    System.err.println("stderr outside try-resources is not collected");

    assertThat(out, contains("out0", "out1", "out2", "out3", "out4"));
    assertThat(err, contains("err0", "err1", "err2", "err3", "err4"));
  }
}
