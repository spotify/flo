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

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;

import java.util.Optional;
import org.junit.Test;

public class EvalContextWithTaskTest {

  EvalContext delegate = mock(EvalContext.class);
  Task<?> task = Task.named("TestTAsk", "a", "b").ofType(String.class).process(() -> "");

  EvalContext sut = EvalContextWithTask.withTask(delegate, task);

  @Test
  public void testEmptyDefault() throws Exception {
    assertThat(EvalContext.inmem().currentTask(), is(Optional.empty()));
  }

  @Test
  public void testCurrentTaskId() throws Exception {
    assertThat(sut.currentTask(), is(Optional.of(task)));
  }
}
