/*-
 * -\-\-
 * flo-freezer
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

package com.spotify.flo.freezer;

import static com.spotify.flo.Util.colored;

import com.spotify.flo.Task;
import com.spotify.flo.TaskId;
import com.spotify.flo.context.InstrumentedContext;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link InstrumentedContext.Listener} that prints to an slf4j Logger.
 */
public class LoggingListener implements InstrumentedContext.Listener {

  private static final Logger LOG = LoggerFactory.getLogger(LoggingListener.class);

  @Override
  public void task(Task<?> task) {
    task.inputs().forEach(
        (upstream) -> LOG.info("{} <- {}", colored(upstream.id()), colored(task.id()))
    );
  }

  @Override
  public void status(TaskId task, Phase phase) {
    LOG.info("{} :: {}", colored(task), phase);
  }

  @Override
  public void close() throws IOException {
  }
}
