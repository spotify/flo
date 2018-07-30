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

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestContextTest {

  private final TestContext.Key<String> foo = TestContext.key("foo");
  private final TestContext.Key<String> bar = TestContext.key("foo", () -> "hello world");

  private TestScope testScope;

  @Before
  public void setUp() throws Exception {
    testScope = FloTesting.scope();
  }

  @After
  public void tearDown() throws Exception {
    testScope.close();
  }

  @Test
  public void shouldReturnNullForUninitializedKey() {
    assertThat(foo.get(), is(nullValue()));
  }

  @Test
  public void shouldUseGetInitializer() {
    assertThat(foo.get(() -> "baz"), is("baz"));
    assertThat(foo.get(), is("baz"));

    assertThat(bar.get(() -> "quux"), is("quux"));
    assertThat(bar.get(), is("quux"));
  }

  @Test
  public void shouldUseDefaultInitializer() {
    assertThat(bar.get(), is("hello world"));
    assertThat(bar.get(() -> "foobar"), is("hello world"));
  }
}
