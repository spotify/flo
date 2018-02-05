/*-
 * -\-\-
 * flo runner
 * --
 * Copyright (C) 2016 Spotify AB
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

import com.spotify.flo.TaskId;
import com.spotify.flo.TaskInfo;
import java.io.Closeable;
import java.io.IOException;

interface Logging extends Closeable {

  default void init() {}

  default void header() {}

  default void willEval(TaskId taskId) {}

  default void startEval(TaskId taskId) {}

  default <T> void completedValue(TaskId taskId, T value) {}

  default void failedValue(TaskId taskId, Throwable valueError) {}

  default void complete(TaskId taskId, long elapsed) {}

  default void exception(Throwable throwable) {}

  default void tree(TaskInfo taskInfo) {}

  default void printPlan(TaskInfo taskInfo) {}

  default void close() throws IOException {}
}
