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
import static org.fusesource.jansi.Ansi.Color.GREEN;
import static org.fusesource.jansi.Ansi.ansi;

import com.spotify.flo.TaskId;
import com.spotify.flo.TaskInfo;
import com.spotify.flo.freezer.Persisted;
import com.spotify.flo.status.TaskStatusException;
import java.io.IOException;
import org.apache.commons.lang.time.DurationFormatUtils;
import org.fusesource.jansi.AnsiConsole;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@code
 * 0         1         2         3         4         5         6         7
 * 012345678901234567890123456789012345678901234567890123456789012345678901234
 * 16:47:42.258 flo-worker-1                 | DEBUG| LockHolder           |> | 74
 * 16:48:18.805 | INFO | EvalContext         |> | 45
 * }
 */
class ConsoleLogging implements Logging {

  private static final Logger LOG = LoggerFactory.getLogger("com.spotify.flo.context.FloRunner");

  @Override
  public void init() {
    AnsiConsole.systemInstall();
  }

  @Override
  public void close() throws IOException {
    AnsiConsole.systemUninstall();
    // TODO: find out what the print below does
    AnsiConsole.out.print(ansi().a("\u001B[?25h"));
    AnsiConsole.out.flush();
  }

  @Override
  public void header() {
    final Package floContextPackage = Package.getPackage("com.spotify.flo.context");
    LOG.info("Runner {}", colored(GREEN, "v" + floContextPackage.getImplementationVersion()));
    LOG.info("");
  }

  @Override
  public void startEval(TaskId taskId) {
    LOG.info("{} Running ...", colored(taskId));
  }

  @Override
  public <T> void completedValue(TaskId taskId, T value) {
    LOG.info("{} Completed -> {}", colored(taskId), value);
  }

  @Override
  public void failedValue(TaskId taskId, Throwable valueError) {
    if (valueError instanceof TaskStatusException) {
      final String exception = valueError.getClass().getSimpleName();
      final int code = ((TaskStatusException) valueError).code();
      LOG.warn("{} Signalled {} -> {}", colored(taskId), exception, code);
    } else if (valueError instanceof Persisted) {
      // ignore
    } else {
      LOG.warn("{} Failed", colored(taskId), valueError);
    }
  }

  @Override
  public void complete(TaskId taskId, long elapsed) {
    LOG.info("Total time {}", DurationFormatUtils.formatDurationHMS(elapsed));
  }

  @Override
  public void exception(Throwable throwable) {
    if (throwable instanceof TaskStatusException) {
      LOG.warn("Could not complete run: {} (code {})",
               throwable.getClass().getSimpleName(),
               ((TaskStatusException) throwable).code());
    } else if (throwable instanceof Persisted) {
      // ignore
    } else {
      LOG.warn("Exception", throwable);
    }
  }

  @Override
  public void tree(TaskInfo taskInfo) {
    PrintUtils.tree(taskInfo).forEach(LOG::info);
  }

  @Override
  public void printPlan(TaskInfo taskInfo) {
    LOG.info("Evaluation plan:");
    PrintUtils.tree(taskInfo).forEach(LOG::info);
    LOG.info("");
  }
}
