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

import static java.util.stream.Collectors.toList;

import com.google.auto.value.AutoValue;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * A materialized, recursive value representation of a {@link Task}.
 *
 * <p>This is a behaviour-less representation of a materialized task graph.
 */
@AutoValue
public abstract class TaskInfo {

  public abstract TaskId id();

  public abstract boolean isReference();

  public abstract List<TaskInfo> inputs();

  public static TaskInfo ref(TaskId id) {
    return new AutoValue_TaskInfo(id, true, Collections.emptyList());
  }

  public static TaskInfo create(TaskId id, List<TaskInfo> upstreams) {
    return new AutoValue_TaskInfo(id, false, upstreams);
  }

  public static TaskInfo ofTask(Task<?> task) {
    return ofTask(task, new HashSet<>());
  }

  public static TaskInfo ofTask(Task<?> task, Set<TaskId> visits) {
    TaskId id = task.id();

    if (visits.contains(id)) {
      return TaskInfo.ref(id);
    } else {
      visits.add(id);
    }

    List<TaskInfo> upstreams = task.inputs().stream()
        .map(t -> ofTask(t, visits))
        .collect(toList());

    return TaskInfo.create(id, upstreams);
  }
}
