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

import io.norberg.automatter.AutoMatter;
import java.util.Base64;
import java.util.List;

@AutoMatter
public interface Workflow {

  List<Task> tasks();

  @AutoMatter
  interface Task {

    String operator();

    String id();

    List<String> upstreams();

    String payloadBase64();

    default byte[] payload() {
      return Base64.getDecoder().decode(payloadBase64());
    }
  }
}
