/*-
 * -\-\-
 * Flo Extras
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

package com.spotify.flo.contrib.beam;

import io.norberg.automatter.AutoMatter;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.runners.dataflow.options.DataflowPipelineWorkerPoolOptions.AutoscalingAlgorithmType;
import org.apache.beam.sdk.PipelineRunner;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;

@AutoMatter
public interface BeamOptions {

  String project();

  int numWorkers();

  String region();

  String zone();

  String stagingLocation();

  String tempLocation();

  String gcpTempLocation();

  AutoscalingAlgorithmType autoscalingAlgorithm();

  int maxNumWorkers();

  String network();

  String subNetwork();

  int diskSizeGb();

  String workerMachineType();

  String jobName();

  String workerDiskType();

  String serviceAccount();

  String bqStagingLocation();

  long bqDefaultExpirationMs();

  Class<PipelineRunner> runner();

  default PipelineOptions options() {
    DataflowPipelineOptions pipelineOptions = PipelineOptionsFactory.create()
        .as(DataflowPipelineOptions.class);

    pipelineOptions.setMaxNumWorkers(5);
    pipelineOptions.setAutoscalingAlgorithm(AutoscalingAlgorithmType.NONE);
    pipelineOptions.setNetwork("default");
    pipelineOptions.setRunner(DataflowRunner.class);

    return pipelineOptions;
  }
}
