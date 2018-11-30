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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

class Version {

  private static final String VERSION_RESOURCE = "/com/spotify/flo/flo-runner.version";

  private static class Lazy {

    private static String RUNNER_VERSION = readFloRunnerVersion();
  }

  static synchronized String floRunnerVersion() {
    return Lazy.RUNNER_VERSION;
  }

  private static String readFloRunnerVersion() {
    try (final BufferedReader reader = new BufferedReader(new InputStreamReader(
        Version.class.getResourceAsStream(VERSION_RESOURCE)))) {
      return reader.readLine().trim();
    } catch (IOException e) {
      throw new RuntimeException(String.format("Failed to read flo runner version from %s", VERSION_RESOURCE), e);
    }
  }
}
