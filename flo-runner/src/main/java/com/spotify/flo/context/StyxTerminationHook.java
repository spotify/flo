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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StyxTerminationHook implements TerminationHook {
  private static final Logger LOG = LoggerFactory.getLogger(StyxTerminationHook.class);

  @Override
  public void accept(Integer exitCode) {
    LOG.info("Termination with code " + exitCode);
    System.exit(exitCode);
  }
}
