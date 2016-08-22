package io.rouz.flo;

import com.google.auto.value.AutoValue;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static java.util.stream.Collectors.toList;

/**
 * A materialized, recursive value representation of a {@link Task}.
 *
 * This is a behaviour-less representation of a materialized task graph.
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
