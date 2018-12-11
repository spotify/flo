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

import static org.apache.commons.lang3.time.DurationFormatUtils.formatDurationHMS;

import com.google.auto.value.AutoValue;
import com.spotify.flo.ControlException;
import com.spotify.flo.TaskId;
import com.spotify.flo.TaskInfo;
import com.spotify.flo.freezer.Persisted;
import com.spotify.flo.status.TaskStatusException;
import java.io.Serializable;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.slf4j.Logger;

class Logging {

  private final Logger LOG;

  private final ConcurrentMap<TaskId, Status> statuses = new ConcurrentHashMap<>();

  private Logging(Logger logger) {
    LOG = logger;
  }

  static Logging create(Logger logger) {
    return new Logging(logger);
  }

  void header() {
    LOG.info("FloRunner v{}", Version.floRunnerVersion());
    LOG.info("");
  }

  void willEval(TaskId id) { }

  void startEval(TaskId taskId) {
    LOG.info("{} Started", taskId);
  }

  <T> void completedValue(TaskId taskId, T value, Duration elapsed) {
    statuses.put(taskId, Status.ofSuccess());
    LOG.info("{} Completed in {} -> {}",
        taskId, formatDurationHMS(elapsed.toMillis()), value);
  }

  <T> void overriddenValue(TaskId taskId, T value) {
    LOG.info("{} has already been computed -> {}", taskId, value);
  }

  void overriddenValueNotFound(TaskId taskId) {
    LOG.info("{} has not previously been computed", taskId);
  }

  void failedValue(TaskId taskId, Throwable valueError, Duration elapsed) {
    statuses.put(taskId, Status.ofFailure(valueError));
    final String hms = formatDurationHMS(elapsed.toMillis());
    if (valueError instanceof TaskStatusException) {
      final String exception = valueError.getClass().getSimpleName();
      LOG.warn("{} Signalled {} after {}", taskId, exception, hms);
    } else if (valueError instanceof Persisted || valueError instanceof ControlException) {
      // ignore
    } else {
      LOG.warn("{} Failed after {}", taskId, hms, valueError);
    }
  }

  void complete(TaskInfo taskInfo, Duration elapsed) {
    LOG.info("Total time {}", formatDurationHMS(elapsed.toMillis()));

    LOG.info("Executed {} out of {} tasks:", statuses.size(), PrintUtils.tree(taskInfo).size());
    PrintUtils.traverseTree(taskInfo, (taskId, s) -> {
      final Status status = statuses.get(taskId);
      final String statusLine;
      if (status == null) {
        statusLine = "Pending";
      } else if (status.success()) {
        statusLine = "Success";
      } else {
        statusLine = "Failure: " + status.failure().map(Throwable::toString).orElse("Unknown Error");
      }
      LOG.info("{}: {}", s, statusLine);
    });
  }

  void exception(Throwable throwable) {
    if (throwable instanceof TaskStatusException) {
      LOG.warn("Could not complete run: {}", throwable.getClass().getSimpleName());
    } else if (throwable instanceof Persisted) {
      // ignore
    } else {
      LOG.warn("Exception", throwable);
    }
  }

  void tree(TaskInfo taskInfo) {
    PrintUtils.tree(taskInfo).forEach(LOG::info);
  }

  void printPlan(TaskInfo taskInfo) {
    LOG.info("Evaluation plan:");
    PrintUtils.tree(taskInfo).forEach(LOG::info);
    LOG.info("");
  }

  @AutoValue
  static abstract class Status {
    boolean success() {
      return !failure().isPresent();
    }
    abstract Optional<Throwable> failure();

    static Status ofSuccess() {
      return new AutoValue_Logging_Status(Optional.empty());
    }
    static Status ofFailure(Throwable t) {
      return new AutoValue_Logging_Status(Optional.of(t));
    }
  }
}
