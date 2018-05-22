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

import static com.spotify.flo.contrib.styx.LabelUtil.buildLabels;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.norberg.automatter.AutoMatter;
import java.util.function.Supplier;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;

@AutoMatter
public interface LabeledPipelineOptionsSupplier extends Supplier<DataflowPipelineOptions> {

  String DEFAULT_LABEL_PREFIX = "spotify";

  String labelPrefix();

  Supplier<DataflowPipelineOptions> supplier();

  Config config();

  default DataflowPipelineOptions get() {
    final DataflowPipelineOptions pipelineOptions = supplier().get();
    pipelineOptions.setLabels(buildLabels(labelPrefix(), config()));
    return pipelineOptions;
  }

  static LabeledPipelineOptionsSupplierBuilder defaultBuilder() {
    return new LabeledPipelineOptionsSupplierBuilder()
        .labelPrefix(DEFAULT_LABEL_PREFIX)
        .config(ConfigFactory.load());
  }

  static LabeledPipelineOptionsSupplierBuilder builder() {
    return new LabeledPipelineOptionsSupplierBuilder();
  }
}
