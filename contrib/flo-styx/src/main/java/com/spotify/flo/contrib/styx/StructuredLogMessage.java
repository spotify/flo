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
import javax.annotation.Nullable;

@AutoMatter
interface StructuredLogMessage {
  @JsonProperty @Nullable String time();
  @JsonProperty @Nullable String severity();
  @JsonProperty @Nullable String logger();
  @JsonProperty @Nullable String thread();
  @JsonProperty @Nullable String message();
  @JsonProperty @Nullable String framework();
  @JsonProperty @Nullable String styx_component_id();
  @JsonProperty @Nullable String styx_workflow_id();
  @JsonProperty @Nullable String styx_docker_args();
  @JsonProperty @Nullable String styx_docker_image();
  @JsonProperty @Nullable String styx_commit_sha();
  @JsonProperty @Nullable String styx_parameter();
  @JsonProperty @Nullable String styx_execution_id();
  @JsonProperty @Nullable String styx_trigger_id();
  @JsonProperty @Nullable String styx_trigger_type();
  @JsonProperty @Nullable String task_id();
  @JsonProperty @Nullable String task_name();
  @JsonProperty @Nullable String task_args();

  StructuredLogMessageBuilder builder();

  static StructuredLogMessageBuilder newBuilder() {
    return new StructuredLogMessageBuilder().framework("flo");
  }
}
