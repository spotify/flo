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

import com.spotify.flo.TaskBuilder.F0;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class TestContext {

  TestContext() {
  }

  private final ConcurrentMap<Key<?>, Object> values = new ConcurrentHashMap<>();

  public static <T> Key<T> key(String name) {
    return new Key<>(name);
  }

  public static <T> Key<T> key(String name, F0<T> initializer) {
    return new Key<>(name, initializer);
  }

  private Object lookup(Key<?> key) {
    return values.get(key);
  }

  private Object lookup(Key<?> key, F0<?> supplier) {
    return values.computeIfAbsent(key, k -> supplier.get());
  }

  static class Key<T> {

    private final String name;
    private final F0<T> initializer;

    public Key(String name) {
      this.name = Objects.requireNonNull(name, "name");
      this.initializer = null;
    }

    public Key(String name, F0<T> initializer) {
      this.name = Objects.requireNonNull(name, "name");
      this.initializer = Objects.requireNonNull(initializer, "initializer");
    }

    @SuppressWarnings("unchecked")
    public T get() {
      if (initializer != null) {
        return (T) FloTesting.context().lookup(this, initializer);
      } else {
        return (T) FloTesting.context().lookup(this);
      }
    }

    @SuppressWarnings("unchecked")
    public T get(F0<T> supplier) {
      return (T) FloTesting.context().lookup(this, supplier);
    }

    @Override
    public String toString() {
      return "Key{" +
          "name='" + name + '\'' +
          '}';
    }
  }
}
