/*-
 * -\-\-
 * Flo Workflow Definition
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

package com.spotify.flo;

public abstract class TaskOperator<T> extends TaskContextGeneric<T> {

  public abstract <U> U run(Task<?> task, T t);

  public static class SpecException extends Error {

    // XXX: exceptions can not be generic
    private final Object spec;

    public SpecException(Object spec) {
      this.spec = spec;
    }

    @SuppressWarnings("unchecked")
    public <T> T spec() {
      return (T) spec;
    }
  }
}
