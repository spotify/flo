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
import org.apache.beam.sdk.options.PipelineOptions;

/**
 * A supplier injecting labels to Dataflow job. This only supports {@link DataflowPipelineOptions}.
 */
@AutoMatter
public interface LabeledPipelineOptionsSupplier extends Supplier<PipelineOptions> {

  String DEFAULT_LABEL_PREFIX = "spotify";

  /**
   * Prefix of the label key.
   */
  String labelKeyPrefix();

  /**
   * The underlying {@link Supplier} supplying {@link PipelineOptions}.
   */
  Supplier<PipelineOptions> supplier();

  /**
   * The config object from which label values are read. If not set, default to
   * ConfigFactory.load().
   */
  Config config();

  /**
   * Build {@link PipelineOptions} using underlying {@link Supplier} and inject labels.
   *
   * @return {@link PipelineOptions} with injected labels
   */
  default PipelineOptions get() {
    final DataflowPipelineOptions pipelineOptions =
        supplier().get().as(DataflowPipelineOptions.class);
    pipelineOptions.setLabels(buildLabels(labelKeyPrefix(), config()));
    return pipelineOptions;
  }

  /**
   * Return a builder with label key prefix set to "spotify", and loading config using
   * ConfigFactory.load().
   *
   * @return a build with label key prefix set to "spotify", and loading config using
   *     ConfigFactory.load().
   */
  static LabeledPipelineOptionsSupplierBuilder builder() {
    return new LabeledPipelineOptionsSupplierBuilder()
        .labelKeyPrefix(DEFAULT_LABEL_PREFIX)
        .config(ConfigFactory.load());
  }
}
