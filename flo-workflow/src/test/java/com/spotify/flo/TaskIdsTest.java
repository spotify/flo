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

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertThat;

import org.junit.Test;

public class TaskIdsTest {

  @Test
  public void shouldHaveHumanReadableToString() {
    TaskId taskId = TaskId.create("MyTask", "a", 1, 2.3, new Param(7));

    assertThat(taskId.toString(), startsWith("MyTask(a,1,2.3,Param{arg=7})"));
  }

  @Test
  public void shouldHaveIdentity() {
    TaskId taskId1 = TaskId.create("MyTask", "a", 1, 2.3, new Param(7));
    TaskId taskId2 = TaskId.create("MyTask", "a", 1, 2.3, new Param(7));

    assertThat(taskId1, not(sameInstance(taskId2)));
    assertThat(taskId1, equalTo(taskId2));
    assertThat(taskId2, equalTo(taskId1));
  }

  @Test
  public void parsedIdEqualToActual() throws Exception {
    TaskId taskId1 = TaskId.create("MyTask", "a", 1, 2.3, new Param(7));
    TaskId taskId2 = TaskId.parse(taskId1.toString());

    assertThat(taskId1, not(sameInstance(taskId2)));
    assertThat(taskId1, equalTo(taskId2));
    assertThat(taskId2, equalTo(taskId1));
  }

  @Test
  public void shouldParseBigHash() throws Exception {
    TaskId taskId = TaskId.parse("FooBar(a,1)#8adaddda");

    assertThat(taskId.hash(), equalTo(-1965367846));
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldNotAllowOpenParenthesisInName() throws Exception {
    TaskId.create("MyTa(sk");
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldNotAllowCloseParenthesisInName() throws Exception {
    TaskId.create("MyTa)sk");
  }

  @Test(expected = NumberFormatException.class)
  public void shouldNotParseInvalidIdHash() throws Exception {
    TaskId.parse("Wat(123)#hello");
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldNotParseInvalidName() throws Exception {
    TaskId.parse("Wat)#(123)#beef");
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldNotParseInvalidParens() throws Exception {
    TaskId.parse("Wat)foo(#beef");
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldNotParseInvalidParensNoOpen() throws Exception {
    TaskId.parse("Wat)foo#beef");
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldNotParseInvalidHashPlacement() throws Exception {
    TaskId.parse("Wat(#)beef");
  }

  private static class Param {

    private final int arg;

    private Param(int arg) {
      this.arg = arg;
    }

    @Override
    public String toString() {
      return "Param{" +
             "arg=" + arg +
             '}';
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      Param param = (Param) o;

      return arg == param.arg;

    }

    @Override
    public int hashCode() {
      return arg;
    }
  }
}
