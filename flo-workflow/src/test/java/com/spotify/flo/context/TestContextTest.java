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

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;

import com.spotify.flo.context.TestContext.Value;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestContextTest {

  private final TestValue BAR = new TestValue("bar");
  private final TestValue BAZ = new TestValue("baz");
  private final TestValue QUUX = new TestValue("quux");

  private final TestContext.Key<TestValue> foo =
      TestContext.key(TestContextTest.class, "foo");
  private final TestContext.Key<TestValue> bar =
      TestContext.key(TestContextTest.class, "bar", () -> BAR);

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
    assertThat(foo.get(() -> BAZ), is(BAZ));
    assertThat(foo.get(), is(BAZ));

    assertThat(bar.get(() -> QUUX), is(QUUX));
    assertThat(bar.get(), is(QUUX));
  }

  @Test
  public void shouldUseDefaultInitializer() {
    assertThat(bar.get(), is(BAR));
    assertThat(bar.get(() -> new TestValue("foobar")), is(BAR));
  }

  class TestValue implements Value<TestValue> {

    private final String name;

    TestValue(String name) {
      this.name = name;
    }

    @Override
    public TestValue withOutputAdded(TestValue other) {
      return new TestValue(name);
    }

    @Override
    public TestValue asInput() {
      return new TestValue(name);
    }
  }
}
