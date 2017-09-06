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

package com.spotify.flo;

/**
 * A type-safe key for metadata key-value pairs that can be added to a {@link Task}.
 *
 * @param <E> The type of the values associated with this key
 */
public interface MetaKey<E> {

  String name();

  /**
   * Should be used to create metadata entries from operators in {@link OpProvider#provideMeta()}.
   *
   * @param value The value for the created entry
   * @return A metadata entry
   */
  default Entry<E> value(E value) {
    return new Entry<E>() {
      @Override
      public MetaKey<E> key() {
        return MetaKey.this;
      }

      @Override
      public E value() {
        return value;
      }
    };
  }

  /**
   * Create a task metadata key with a given name. Use this static constructor to create key names.
   *
   * <p>Keys should ideally be created as static constants.
   *
   * <pre>{@code
   * public static final MetaKey<String> SOME_METADATA = MetaKey.create("Metadata foo");
   * }</pre>
   */
  static <E> MetaKey<E> create(String name) {
    return () -> name;
  }

  /**
   * A type-safe key-value pair for some {@link MetaKey}. These can be created by calling
   * {@link MetaKey#value(Object)}.
   *
   * @param <E> The type of the value
   */
  interface Entry<E> {
    MetaKey<E> key();
    E value();
  }
}
