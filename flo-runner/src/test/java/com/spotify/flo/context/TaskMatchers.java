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

import com.spotify.flo.Task;
import com.spotify.flo.TaskId;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

class TaskMatchers {

  static Matcher<Task> isTaskWithId(TaskId id) {
    return new TypeSafeMatcher<Task>() {
      @Override
      protected boolean matchesSafely(Task item) {
        return id.equals(item.id());
      }

      @Override
      public void describeTo(Description description) {
        description.appendText("a task with id ").appendValue(id);
      }
    };
  }
}
