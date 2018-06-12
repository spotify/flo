/*-
 * -\-\-
 * flo-styx
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

package com.spotify.flo.contrib.styx;

import com.spotify.flo.context.TerminationHook;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TerminationLogging implements TerminationHook {

  private static final Logger LOG = LoggerFactory.getLogger(TerminationLogging.class);

  @Override
  public void accept(Integer exitCode) {
    final Map<String, String> env = System.getenv();

    final String terminationLog = env.get("STYX_TERMINATION_LOG");

    // make termination logging optional
    if (terminationLog == null) {
      return;
    }

    final String content = String.format("{\"component_id\": \"%s\","
                                         + "\"workflow_id\": \"%s\","
                                         + "\"parameter\": \"%s\","
                                         + "\"execution_id\": \"%s\","
                                         + "\"event\": \"exited\","
                                         + "\"exit_code\": %d}",
        env.getOrDefault("STYX_COMPONENT_ID", "UNKNOWN_COMPONENT_ID"),
        env.getOrDefault("STYX_WORKFLOW_ID", "UNKNOWN_WORKFLOW_ID"),
        env.getOrDefault("STYX_PARAMETER", "UNKNOWN_PARAMETER"),
        env.getOrDefault("STYX_EXECUTION_ID", "UNKNOWN_EXECUTION_ID"),
        exitCode);
    final Path path = Paths.get(terminationLog);

    try {
      Files.write(path, content.getBytes(),
          StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
    } catch (IOException e) {
      LOG.error("Could not write termination log to {}.", path, e);
      throw new RuntimeException(e);
    }
  }
}
