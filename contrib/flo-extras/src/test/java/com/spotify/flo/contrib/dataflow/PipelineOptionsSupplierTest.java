/*-
 * -\-\-
 * Spotify flo
 * --
 * Copyright (C) 2018 Spotify AB
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
import static org.junit.Assert.assertThat;

import java.util.Optional;
import org.apache.beam.runners.dataflow.DataflowRunner;
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
        .build();
    assertThat(pipelineOptionsSupplier.project(), is("foo"));
    assertThat(pipelineOptionsSupplier.maxNumWorkers(), is(Optional.of(5)));
    assertThat(pipelineOptionsSupplier.network(), is(Optional.of("default")));
    assertThat(pipelineOptionsSupplier.autoscalingAlgorithm(),
        is(Optional.of(AutoscalingAlgorithmType.NONE)));
    assertThat(pipelineOptionsSupplier.runner(), is(Optional.of(DataflowRunner.class)));
  }
}
