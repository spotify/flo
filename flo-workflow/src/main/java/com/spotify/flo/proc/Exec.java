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

package com.spotify.flo.proc;

import com.google.auto.value.AutoValue;
import com.spotify.flo.TaskBuilder.F1;
import com.spotify.flo.TaskBuilder.F2;
import com.spotify.flo.TaskBuilder.F3;
import java.util.Arrays;

/**
 * Experimental
 */
public final class Exec {

  private Exec() {
    // no instantiation
  }

  public static <A> F1<A, Result> exec(F1<A, String[]> f) {
    return a -> {
      final String[] args = f.apply(a);
      System.out.println("running " + Arrays.toString(args));
      // exec(args);
      return new AutoValue_Exec_ResultValue(args.length);
    };
  }

  public static <A, B> F2<A, B, Result> exec(F2<A, B, String[]> f) {
    return (a, b) -> {
      final String[] args = f.apply(a, b);
      System.out.println("running " + Arrays.toString(args));
      // exec(args);
      return new AutoValue_Exec_ResultValue(args.length);
    };
  }

  public static <A, B, C> F3<A, B, C, Result> exec(F3<A, B, C, String[]> f) {
    return (a, b, c) -> {
      final String[] args = f.apply(a, b, c);
      System.out.println("running " + Arrays.toString(args));
      // exec(args);
      return new AutoValue_Exec_ResultValue(args.length);
    };
  }

  public interface Result {
    int exitCode();
  }

  @AutoValue
  static abstract class ResultValue implements Result {
  }
}
