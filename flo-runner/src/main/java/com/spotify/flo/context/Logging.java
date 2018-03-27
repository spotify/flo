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

import static com.spotify.flo.Util.colored;
import static org.apache.commons.lang3.time.DurationFormatUtils.formatDurationHMS;
import static org.fusesource.jansi.Ansi.Color.GREEN;

import com.spotify.flo.TaskId;
import com.spotify.flo.TaskInfo;
import com.spotify.flo.freezer.Persisted;
import com.spotify.flo.status.TaskStatusException;
import java.time.Duration;
import org.slf4j.Logger;

public class Logging {

  private final Logger LOG;

  private Logging(Logger logger) {
    LOG = logger;
  }

  static Logging create(Logger logger) {
    return new Logging(logger);
  }

  void header() {
    LOG.info("Runner {}", colored(GREEN, "v" + getClass().getPackage().getImplementationVersion()));
    LOG.info("");
  }

  void willEval(TaskId id) { }

  void startEval(TaskId taskId) {
    LOG.info("{} Started", colored(taskId));
  }

  <T> void completedValue(TaskId taskId, T value, Duration elapsed) {
    LOG.info("{} Completed in {} -> {}",
        colored(taskId), formatDurationHMS(elapsed.toMillis()), value);
  }

  <T> void overriddenValue(TaskId taskId, T value) {
    LOG.info("{} has already been computed -> {}", colored(taskId), value);
  }

  void overriddenValueNotFound(TaskId taskId) {
    LOG.info("{} has not previously been computed", colored(taskId));
  }

  void failedValue(TaskId taskId, Throwable valueError, Duration elapsed) {
    final String hms = formatDurationHMS(elapsed.toMillis());
    if (valueError instanceof TaskStatusException) {
      final String exception = valueError.getClass().getSimpleName();
      LOG.warn("{} Signalled {} after {}", colored(taskId), exception, hms);
    } else if (valueError instanceof Persisted) {
      // ignore
    } else {
      LOG.warn("{} Failed after {}", colored(taskId), hms, valueError);
    }
  }

  void complete(TaskId taskId, Duration elapsed) {
    LOG.info("Total time {}", formatDurationHMS(elapsed.toMillis()));
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
}
