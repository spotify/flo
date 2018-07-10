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

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class ForkingExecutorTest {

  @Rule public ExpectedException exception = ExpectedException.none();

  private ForkingExecutor forkingExecutor;

  @Before
  public void setUp() {
    forkingExecutor = new ForkingExecutor();
  }

  @Test
  public void returnsResult() throws IOException {
    final String result = forkingExecutor.execute(() ->
        "hello world!");
    assertThat(result, is("hello world!"));
  }

  @Test
  public void propagatesException() throws IOException {
    exception.expect(FoobarException.class);
    exception.expectMessage("foobar!");
    forkingExecutor.execute(() -> {
      throw new FoobarException("foobar!");
    });
  }

  @Test
  public void captures() throws IOException {
    final Map<String, String> map = new HashMap<>();
    map.put("foo", "bar");
    final Map<String, String> result = forkingExecutor.execute(() ->
        map);
    assertThat(result, is(map));
  }

  @Test
  public void executesInSubprocess() throws IOException {
    final String thisJvm = ManagementFactory.getRuntimeMXBean().getName();
    final String subprocessJvm = forkingExecutor.execute(() ->
        ManagementFactory.getRuntimeMXBean().getName());
    assertThat(thisJvm, is(not(subprocessJvm)));
  }

  @Test
  public void setsEnvironment() throws IOException {
    final String result = forkingExecutor
        .environment(Collections.singletonMap("foo", "bar"))
        .execute(() -> System.getenv("foo"));
    assertThat(result, is("bar"));
  }

  @Test
  public void setsJavaArgs() throws IOException {
    final String result = forkingExecutor
        .javaArgs("-Dfoo=bar")
        .execute(() -> System.getProperty("foo"));
    assertThat(result, is("bar"));
  }

  @Test
  public void propagatesJavaArgs() throws IOException {
    final String result = forkingExecutor
        .javaArgs("-Dfoo=bar")
        .execute(() -> {
          // Fork again with an executor without -Dfoo=bar explicitly configured
          try (ForkingExecutor inner = new ForkingExecutor()) {
            // And check that -Dfoo=bar was automatically propagated
            return inner.execute(() -> System.getProperty("foo"));
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        });
    assertThat(result, is("bar"));
  }

  private static class FoobarException extends RuntimeException {

    FoobarException(String message) {
      super(message);
    }
  }
}