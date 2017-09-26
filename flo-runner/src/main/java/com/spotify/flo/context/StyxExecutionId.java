/*-
 * -\-\-
 * flo runner
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

package com.spotify.flo.context;

import com.google.auto.value.AutoValue;
import com.typesafe.config.Config;

@AutoValue
public abstract class StyxExecutionId {

  public abstract String workflowId();
  public abstract String parameter();
  public abstract String executionId();

  public static StyxExecutionId fromConfig(Config config) {
    final String workflowId = config.getString("styx.workflow.id");
    final String parameter = config.getString("styx.workflow.parameter");
    final String execId = config.getString("styx.workflow.execution");

    return new AutoValue_StyxExecutionId(workflowId, parameter, execId);
  }
}
