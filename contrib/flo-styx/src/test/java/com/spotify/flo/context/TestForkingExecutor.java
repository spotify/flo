/*-
 * -\-\-
 * Flo Styx
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

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.Serializable;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Callable;

public class TestForkingExecutor {

  public static <T> Result<T> execute(Map<String, String> env, SerializableCallable<T> f) throws IOException {
    try (final ForkingExecutor executor = new ForkingExecutor();
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        final ByteArrayOutputStream err = new ByteArrayOutputStream()) {
      final T result;
      try (final PrintStream pout = new PrintStream(out);
          final PrintStream perr = new PrintStream(err)) {
        executor.out(pout);
        executor.err(perr);
        executor.environment(env);
        result = executor.execute(() -> {
          try {
            return f.call();
          } catch (Error | RuntimeException e) {
            throw e;
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        });
      }
      return new Result(result, out.toByteArray(), err.toByteArray());
    }
  }

  public static class Result<T> {

    public final T result;
    public final byte[] out;
    public final byte[] err;

    public Result(T result, byte[] out, byte[] err) {
      this.result = Objects.requireNonNull(result);
      this.out = Objects.requireNonNull(out);
      this.err = Objects.requireNonNull(err);
    }

    public String outUtf8() {
      return new String(out, UTF_8);
    }

    public String errUtf8() {
      return new String(err, UTF_8);
    }
  }

  public interface SerializableCallable<V> extends Callable<V>, Serializable {

  }
}
