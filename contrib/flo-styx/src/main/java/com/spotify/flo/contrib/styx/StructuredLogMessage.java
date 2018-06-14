/*-
 * -\-\-
 * Flo Styx
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

import com.fasterxml.jackson.annotation.JsonProperty;
import io.norberg.automatter.AutoMatter;

@AutoMatter
interface StructuredLogMessage {
  @AutoMatter
  interface Workflow {
    @JsonProperty String styx_component_id();
    @JsonProperty String styx_workflow_id();
    @JsonProperty String styx_docker_args();
    @JsonProperty String styx_docker_image();
    @JsonProperty String styx_commit_sha();
    @JsonProperty String styx_parameter();
    @JsonProperty String styx_execution_id();
    @JsonProperty String styx_trigger_id();
    @JsonProperty String styx_trigger_type();
    @JsonProperty String framework();
    @JsonProperty String task_id();
    @JsonProperty String task_name();
    @JsonProperty String task_args();

    static WorkflowBuilder newBuilder() {
      return new WorkflowBuilder().framework("flo");
    }
  }

  @JsonProperty String time();
  @JsonProperty String severity();
  @JsonProperty String logger();
  @JsonProperty String thread();
  @JsonProperty String message();
  @JsonProperty Workflow workflow();

  static StructuredLogMessageBuilder newBuilder() {
    return new StructuredLogMessageBuilder();
  }
}
