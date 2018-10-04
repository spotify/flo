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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.UncheckedIOException;
import java.util.function.Consumer;

class ThreadAwarePrintStream extends PrintStream {

  private final ThreadLocal<OutputStream> outputStream;

  private ThreadAwarePrintStream(String threadNamePrefix,
                                 OutputStream actualOutputStream,
                                 Consumer<String> consumer) {
    super(actualOutputStream);

    outputStream = ThreadLocal.withInitial(() -> {
      if (!Thread.currentThread().getName().startsWith(threadNamePrefix)) {
        return actualOutputStream;
      }

      return new OutputStream() {
        private final ByteArrayOutputStream baos = new ByteArrayOutputStream(4096);

        @Override
        public void write(int b) {
          if (b == '\n') {
            final String line = baos.toString();
            baos.reset();
            consumer.accept(line);
          } else {
            baos.write(b);
          }
        }
      };
    });
  }

  @Override
  public void write(byte[] b, int off, int len) {
    try {
      outputStream.get().write(b, off, len);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  public static Overrider override(String threadNamePrefix,
                                   Consumer<String> outConsumer,
                                   Consumer<String> errConsumer) {
    return Overrider.create(threadNamePrefix, outConsumer, errConsumer);
  }

  public static class Overrider implements AutoCloseable {
    private PrintStream out;
    private PrintStream err;

    private Overrider() {
      out = System.out;
      err = System.err;
    }

    private static Overrider create(String threadNamePrefix,
                                    Consumer<String> outConsumer,
                                    Consumer<String> errConsumer) {
      final Overrider overrider = new Overrider();

      final ThreadAwarePrintStream out =
          new ThreadAwarePrintStream(threadNamePrefix, overrider.out, outConsumer);
      System.setOut(out);

      final ThreadAwarePrintStream err =
          new ThreadAwarePrintStream(threadNamePrefix, overrider.err, errConsumer);
      System.setErr(err);

      return overrider;
    }

    @Override
    public void close() {
      System.setOut(out);
      System.setErr(err);
    }
  }
}
