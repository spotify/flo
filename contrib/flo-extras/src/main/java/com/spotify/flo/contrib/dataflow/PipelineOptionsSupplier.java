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

package com.spotify.flo.contrib.dataflow;

import io.norberg.automatter.AutoMatter;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.runners.dataflow.options.DataflowPipelineWorkerPoolOptions.AutoscalingAlgorithmType;
import org.apache.beam.sdk.PipelineRunner;
import org.apache.beam.sdk.options.PipelineOptionsFactory;

@AutoMatter
public interface PipelineOptionsSupplier extends Supplier<DataflowPipelineOptions> {

  String project();

  Optional<Integer> numWorkers();

  Optional<Integer> maxNumWorkers();

  Optional<String> region();

  Optional<String> zone();

  Optional<String> stagingLocation();

  Optional<String> tempLocation();

  Optional<String> gcpTempLocation();

  Optional<AutoscalingAlgorithmType> autoscalingAlgorithm();

  Optional<String> network();

  Optional<String> subNetwork();

  Optional<Integer> diskSizeGb();

  Optional<String> workerMachineType();

  Optional<String> workerDiskType();

  Optional<String> jobName();

  String serviceAccount();

  Optional<Class<? extends PipelineRunner<?>>> runner();

  Optional<List<String>> experiment();

  static PipelineOptionsSupplierBuilder builder() {
    return new PipelineOptionsSupplierBuilder()
        .autoscalingAlgorithm(AutoscalingAlgorithmType.NONE)
        .runner(DataflowRunner.class);
  }

  default DataflowPipelineOptions get() {
    final DataflowPipelineOptions pipelineOptions = PipelineOptionsFactory.create()
        .as(DataflowPipelineOptions.class);

    pipelineOptions.setProject(project());

    numWorkers().ifPresent(pipelineOptions::setNumWorkers);
    maxNumWorkers().ifPresent(pipelineOptions::setMaxNumWorkers);

    region().ifPresent(pipelineOptions::setRegion);
    zone().ifPresent(pipelineOptions::setZone);

    stagingLocation().ifPresent(pipelineOptions::setStagingLocation);
    tempLocation().ifPresent(pipelineOptions::setTempLocation);
    gcpTempLocation().ifPresent(pipelineOptions::setGcpTempLocation);

    autoscalingAlgorithm().ifPresent(pipelineOptions::setAutoscalingAlgorithm);
    network().ifPresent(pipelineOptions::setNetwork);
    subNetwork().ifPresent(pipelineOptions::setSubnetwork);

    diskSizeGb().ifPresent(pipelineOptions::setDiskSizeGb);
    workerMachineType().ifPresent(pipelineOptions::setWorkerMachineType);
    workerDiskType().ifPresent(pipelineOptions::setWorkerDiskType);

    jobName().ifPresent(pipelineOptions::setJobName);
    pipelineOptions.setServiceAccount(serviceAccount());

    experiment().ifPresent(pipelineOptions::setExperiments);

    runner().ifPresent(pipelineOptions::setRunner);

    return pipelineOptions;
  }
}
