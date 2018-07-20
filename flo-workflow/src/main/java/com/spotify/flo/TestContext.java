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

  private final ConcurrentMap<Key<?>, Value<?>> values = new ConcurrentHashMap<>();

  public static <T extends Value<T>> Key<T> key(Class<?> cls, String name) {
    return new Key<>(cls, name);
  }

  public static <T extends Value<T>> Key<T> key(Class<?> cls, String name, F0<T> initializer) {
    return new Key<>(cls, name, initializer);
  }

  @SuppressWarnings("unchecked")
  private <T extends Value<T>> Value<T> lookup(Key<T> key) {
    return (Value<T>) values.get(key);
  }

  @SuppressWarnings("unchecked")
  private <T extends Value<T>> Value<T>  lookup(Key<T> key, F0<T> supplier) {
    return (Value<T>) values.computeIfAbsent(key, k -> supplier.get());
  }

  public TestContext inputOnly() {
    final TestContext inputOnly = new TestContext();
    values.forEach((Key<?> key, Value value) -> inputOnly.values.put(key, value.inputOnly()));
    return inputOnly;
  }

  @SuppressWarnings("unchecked")
  public void mergeOutput(TestContext testContext) {
    testContext.values.forEach((Key<?> key, Value value) -> {
      values.compute(key, (Key<?> k, Value currentValue) -> {
        if (currentValue != null) {
          return currentValue.mergedOutput(value);
        } else {
          return value;
        }
      });
    });
  }

  @Override
  public String toString() {
    return "TestContext{" +
        "values=" + values +
        '}';
  }

  public static class Key<T extends Value<T>> {

    private final String cls;
    private final String name;
    private final F0<T> initializer;

    Key(Class<?> cls, String name) {
      this.cls = cls.getCanonicalName();
      this.name = Objects.requireNonNull(name, "name");
      this.initializer = null;
    }

    Key(Class<?> cls, String name, F0<T> initializer) {
      this.cls = cls.getCanonicalName();
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
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      Key<?> key = (Key<?>) o;
      return Objects.equals(cls, key.cls) &&
          Objects.equals(name, key.name);
    }

    @Override
    public int hashCode() {
      return Objects.hash(cls, name);
    }

    @Override
    public String toString() {
      return "Key{" +
          "cls='" + cls + '\'' +
          ", name='" + name + '\'' +
          '}';
    }
  }

  public interface Value<T extends Value> {

    T mergedOutput(T other);

    T inputOnly();
  }
}
