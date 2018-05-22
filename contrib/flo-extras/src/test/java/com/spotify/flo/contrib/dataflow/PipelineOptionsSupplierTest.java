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

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.typeCompatibleWith;
import static org.junit.Assert.assertThat;

import com.google.common.collect.ImmutableList;
import java.util.Optional;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.runners.dataflow.options.DataflowPipelineWorkerPoolOptions.AutoscalingAlgorithmType;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class PipelineOptionsSupplierTest {

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Test
  public void shouldThrowNPEIfNotSettingProject() {
    expectedException.expect(NullPointerException.class);
    expectedException.expectMessage("project");

    PipelineOptionsSupplier.builder().build();
  }

  @Test
  public void shouldBuildDefaultSupplier() {
    final PipelineOptionsSupplier pipelineOptionsSupplier = PipelineOptionsSupplier.builder()
        .project("foo")
        .serviceAccount("foo@example.com")
        .build();
    assertThat(pipelineOptionsSupplier.project(), is("foo"));
    assertThat(pipelineOptionsSupplier.serviceAccount(), is("foo@example.com"));
    assertThat(pipelineOptionsSupplier.runner(), is(Optional.of(DataflowRunner.class)));
  }
  
  @Test
  public void shouldSupplyDefaultPipelineOptions() {
    final DataflowPipelineOptions pipelineOptions = PipelineOptionsSupplier.builder()
        .project("foo")
        .serviceAccount("foo@example.com")
        .build()
        .get()
        .as(DataflowPipelineOptions.class);
    assertThat(pipelineOptions.getProject(), is("foo"));
    assertThat(pipelineOptions.getServiceAccount(), is("foo@example.com"));
    assertThat(pipelineOptions.getRunner(), typeCompatibleWith(DataflowRunner.class));
  }

  @Test
  public void shouldSupplyPipelineOptionsAsConfigured() {
    final DataflowPipelineOptions pipelineOptions = PipelineOptionsSupplier.builder()
        .project("foo")
        .maxNumWorkers(1)
        .region("foo")
        .zone("bar")
        .stagingLocation("gs://foo")
        .tempLocation("gs://bar")
        .gcpTempLocation("gs://foobar")
        .autoscalingAlgorithm(AutoscalingAlgorithmType.THROUGHPUT_BASED)
        .network("foo")
        .subNetwork("foobar")
        .diskSizeGb(100)
        .workerDiskType("foo")
        .jobName("foo")
        .serviceAccount("foo@example.com")
        .runner(DataflowRunner.class)
        .experiment(ImmutableList.of("foo", "bar"))
        .build()
        .get()
        .as(DataflowPipelineOptions.class);
    assertThat(pipelineOptions.getProject(), is("foo"));
    assertThat(pipelineOptions.getMaxNumWorkers(), is(1));
    assertThat(pipelineOptions.getRegion(), is("foo"));
    assertThat(pipelineOptions.getZone(), is("bar"));
    assertThat(pipelineOptions.getStagingLocation(), is("gs://foo"));
    assertThat(pipelineOptions.getTempLocation(), is("gs://bar"));
    assertThat(pipelineOptions.getGcpTempLocation(), is("gs://foobar"));
    assertThat(pipelineOptions.getAutoscalingAlgorithm(), is(AutoscalingAlgorithmType.THROUGHPUT_BASED));
    assertThat(pipelineOptions.getNetwork(), is("foo"));
    assertThat(pipelineOptions.getSubnetwork(), is("foobar"));
    assertThat(pipelineOptions.getDiskSizeGb(), is(100));
    assertThat(pipelineOptions.getWorkerDiskType(), is("foo"));
    assertThat(pipelineOptions.getJobName(), is("foo"));
    assertThat(pipelineOptions.getServiceAccount(), is("foo@example.com"));
    assertThat(pipelineOptions.getRunner(), typeCompatibleWith(DataflowRunner.class));
    assertThat(pipelineOptions.getExperiments(), is(ImmutableList.of("foo", "bar")));
  }
}
