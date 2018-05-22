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

/**
 * A supplier of {@link DataflowPipelineOptions}.
 *
 * Please refer to https://cloud.google.com/dataflow/pipelines/specifying-exec-params for details.
 */
@AutoMatter
public interface PipelineOptionsSupplier extends Supplier<DataflowPipelineOptions> {

  /**
   * The project ID for your Google Cloud Project.
   * Mandatory value. Not setting it will result {@link NullPointerException}.
   */
  String project();

  /**
   * Run the job as a specific service account.
   * Mandatory value. Not setting it will result {@link NullPointerException}.
   */
  String serviceAccount();

  /**
   * The initial number of Google Compute Engine instances to use when executing your pipeline.
   */
  Optional<Integer> numWorkers();

  /**
   * The maximum number of Google Compute Engine instances to be made available to your pipeline
   * during execution.
   */
  Optional<Integer> maxNumWorkers();

  /**
   * Specifying a regional endpoint allows you to define a region for deploying your Cloud
   * Dataflow jobs.
   */
  Optional<String> region();

  /**
   * The Compute Engine availability zone for launching worker instances to run your pipeline.
   */
  Optional<String> zone();

  /**
   * Google Cloud Storage path for staging local files. Must be a valid Cloud Storage URL,
   * beginning with gs://.
   */
  Optional<String> stagingLocation();

  /**
   * A Google Cloud Storage path for Dataflow to stage any temporary files.
   */
  Optional<String> tempLocation();

  /**
   * A Google Cloud Storage path for Dataflow to stage any temporary files.
   */
  Optional<String> gcpTempLocation();

  /**
   * The Autoscaling mode to use for your Dataflow job.
   */
  Optional<AutoscalingAlgorithmType> autoscalingAlgorithm();

  /**
   * The Google Compute Engine network for launching Compute Engine instances to run your pipeline.
   */
  Optional<String> network();

  /**
   * The Google Compute Engine subnetwork for launching Compute Engine instances to run your
   * pipeline.
   */
  Optional<String> subNetwork();

  /**
   * The disk size, in gigabytes, to use on each remote Compute Engine worker instance.
   */
  Optional<Integer> diskSizeGb();

  /**
   * The Google Compute Engine machine type that Dataflow will use when spinning up worker VMs.
   */
  Optional<String> workerMachineType();

  /**
   * The type of persistent disk to use, specified by a full URL of the disk type resource.
   */
  Optional<String> workerDiskType();

  /**
   * Name of the pipeline execution.
   */
  Optional<String> jobName();

  /**
   * The PipelineRunner to use.
   */
  Optional<Class<? extends PipelineRunner<?>>> runner();

  /**
   * Apache Beam experimental feature flags.
   */
  Optional<List<String>> experiment();

  /**
   * Return a builder with runner set to {@link DataflowRunner}.
   *
   * @return a builder with runner set to {@link DataflowRunner}
   */
  static PipelineOptionsSupplierBuilder builder() {
    return new PipelineOptionsSupplierBuilder()
        .runner(DataflowRunner.class);
  }

  /**
   * Build {@link DataflowPipelineOptions} based on configured values of this supplier.
   *
   * @return {@link DataflowPipelineOptions}
   */
  default DataflowPipelineOptions get() {
    final DataflowPipelineOptions pipelineOptions = PipelineOptionsFactory.create()
        .as(DataflowPipelineOptions.class);

    pipelineOptions.setProject(project());
    pipelineOptions.setServiceAccount(serviceAccount());
    jobName().ifPresent(pipelineOptions::setJobName);

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

    experiment().ifPresent(pipelineOptions::setExperiments);

    runner().ifPresent(pipelineOptions::setRunner);

    return pipelineOptions;
  }
}
