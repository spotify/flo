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

import static org.junit.Assert.assertTrue;

import com.spotify.flo.context.AwaitingConsumer;

/**
 * Testing utility for waiting for values
 */
public final class AwaitValue<T> extends AwaitingConsumer<T> {

  private String acceptingThreadName;

  @Override
  public void accept(T t) {
    acceptingThreadName = Thread.currentThread().getName();
    super.accept(t);
  }

  @Override
  public T awaitAndGet() throws InterruptedException {
    assertTrue("wait for value", await());
    return super.get();
  }

  public String acceptingThreadName() {
    return acceptingThreadName;
  }
}
