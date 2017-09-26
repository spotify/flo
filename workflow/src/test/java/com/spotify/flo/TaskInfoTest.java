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

import org.junit.Test;

public class TaskInfoTest {

  @Test
  public void testTaskInfo() throws Exception {
    Task<String> task = first();
    TaskInfo taskInfo = TaskInfo.ofTask(task);

    assertThat(taskInfo.id(), is(task.id()));
    assertThat(taskInfo.isReference(), is(false));
    assertThat(taskInfo.inputs().size(), is(1));

    TaskInfo input1 = taskInfo.inputs().get(0);
    assertThat(input1.id(), is(second(1).id()));
    assertThat(input1.isReference(), is(false));
    assertThat(input1.inputs().size(), is(1));

    TaskInfo input2 = input1.inputs().get(0);
    assertThat(input2.id(), is(second(1).id()));
    assertThat(input2.isReference(), is(true));
    assertThat(input2.inputs().size(), is(0));
  }

  private Task<String> first() {
    return Task.named("First").ofType(String.class)
        .in(() -> second(1))
        .process((s) -> "foo");
  }

  private Task<String> second(int i) {
    return Task.named("Second", i).ofType(String.class)
        .in(() -> second(i))
        .process((self) -> "bar");
  }
}
