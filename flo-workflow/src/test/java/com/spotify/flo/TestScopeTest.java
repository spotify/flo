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

import io.grpc.Context;
import org.junit.Test;

public class TestScopeTest {

  private static final Context.Key<String> TEST_KEY = Context.key("test");

  @Test
  public void shouldAttachAndDetach() {
    final Context original = Context.current();
    assertThat(TEST_KEY.get(), is(nullValue()));
    final Context context = Context.current().withValue(TEST_KEY, "foobar");
    try (final TestScope ts = new TestScope(context)) {
      assertThat(Context.current(), is(context));
      assertThat(TEST_KEY.get(), is("foobar"));
    }
    assertThat(Context.current(), is(original));
    assertThat(TEST_KEY.get(), is(nullValue()));
  }
}
