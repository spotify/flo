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

import static com.fasterxml.jackson.databind.DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY;
import static com.fasterxml.jackson.databind.PropertyNamingStrategy.SNAKE_CASE;
import static java.util.stream.Collectors.toList;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.spotify.flo.TaskInfo;
import java.util.List;

/**
 * Logging implementation that only prints out a json tree of the task tree on
 * {@link JsonTreeLogging#tree(TaskInfo)}.
 */
class JsonTreeLogging implements Logging {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
      .setPropertyNamingStrategy(SNAKE_CASE)
      .enable(ACCEPT_SINGLE_VALUE_AS_ARRAY);

  @Override
  public void tree(TaskInfo taskInfo) {
    try {
      final String json = OBJECT_MAPPER.writeValueAsString(TaskInfoJson.create(taskInfo));
      System.out.println(json);
    } catch (JsonProcessingException e) {
      e.printStackTrace();
    }
  }

  private static final class TaskInfoJson {

    @JsonProperty final String id;
    @JsonProperty final List<TaskInfoJson> inputs;
    @JsonProperty final boolean isReference;

    private TaskInfoJson(String id, List<TaskInfoJson> inputs, boolean isReference) {
      this.id = id;
      this.inputs = inputs;
      this.isReference = isReference;
    }

    static TaskInfoJson create(TaskInfo taskInfo) {
      final List<TaskInfoJson> inputs = taskInfo.inputs().stream()
          .map(TaskInfoJson::create)
          .collect(toList());

      return new TaskInfoJson(taskInfo.id().toString(), inputs, taskInfo.isReference());
    }
  }
}
