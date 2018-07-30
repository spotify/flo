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

package com.spotify.flo.context;

import com.spotify.flo.TaskBuilder.F0;
import java.io.Serializable;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Holds mocks with input values and output recordings for task executions.
 */
public class TestContext implements Serializable {

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

  /**
   * Create a replica of this {@link TestContext} with all {@link Value} prepared for a single task execution.
   */
  TestContext asInput() {
    final TestContext input = new TestContext();
    values.forEach((Key<?> key, Value value) ->
        input.values.put(key, value.asInput()));
    return input;
  }

  /**
   * Add output from another test context into this context. Intended to be used for merging output from a single
   * task execution into a main context.
   */
  @SuppressWarnings("unchecked")
  void addOutput(TestContext testContext) {
    testContext.values.forEach((Key<?> key, Value value) -> {
      values.compute(key, (Key<?> k, Value currentValue) -> {
        if (currentValue != null) {
          return currentValue.withOutputAdded(value);
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

  public static class Key<T extends Value<T>> implements Serializable {

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

  /**
   * A {@link TestContext} value that can hold mocked input and record output from a task.
   */
  public interface Value<T extends Value> extends Serializable {

    /**
     * Used to merge output from a task into the main test context.
     */
    T withOutputAdded(T other);

    /**
     * Transform a value with input and output to a value with only input. This might e.g. entail merging recorded
     * output into the mocked input so that the output from one task is visible to a downstream task.
     */
    T asInput();
  }
}
