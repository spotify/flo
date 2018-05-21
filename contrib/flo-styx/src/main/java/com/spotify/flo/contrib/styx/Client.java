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

import com.spotify.flo.contrib.dataflow.PipelineOptionsProvider;
import org.apache.beam.sdk.options.PipelineOptions;

public class Client {

  public static void main(String[] args) {
    final PipelineOptionsProvider foobar = PipelineOptionsProvider.builder()
        .jobName("foobar")
        .build();

    final PipelineOptions options = new LabeledPipelineOptionsProvider(foobar).options();
    // ScioContext(options)
  }

}
