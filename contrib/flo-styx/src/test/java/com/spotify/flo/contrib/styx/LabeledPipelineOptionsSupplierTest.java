/*-
 * -\-\-
 * Flo Styx
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

package com.spotify.flo.contrib.styx;

import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.typesafe.config.Config;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class LabeledPipelineOptionsSupplierTest {
  
  @Mock
  private Supplier<DataflowPipelineOptions> supplier;
  
  @Mock
  private DataflowPipelineOptions dataflowPipelineOptions;
  
  @Mock
  private Config config;
  
  @Before
  public void setUp() {
    when(supplier.get()).thenReturn(dataflowPipelineOptions);
  }

  @Test
  public void shouldBuildFromSupplier() {
    assertThat(LabeledPipelineOptionsSupplier.defaultBuilder().supplier(supplier).build(),
        notNullValue());
  }

  @Test
  public void shouldBuildFromScratch() {
    assertThat(LabeledPipelineOptionsSupplier.builder()
            .labelPrefix("foo")
            .config(config)
            .supplier(supplier)
            .build(),
        notNullValue());
  }
  
  @Test
  public void shouldSupplyLabeledPipelineOptions() {
    final Map<String, String> expected = new HashMap<>();
    expected.put("spotify-styx-component-id", "unknown-component-id");
    expected.put("spotify-styx-workflow-id", "unknown-workflow-id");
    expected.put("spotify-styx-parameter", "unknown-parameter");
    expected.put("spotify-styx-execution-id", "unknown-execution-id");
    expected.put("spotify-styx-trigger-id", "unknown-trigger-id");
    expected.put("spotify-styx-trigger-type", "unknown-trigger-type");

    final DataflowPipelineOptions dataflowPipelineOptions =
        LabeledPipelineOptionsSupplier.defaultBuilder().supplier(supplier).build().get();
    verify(dataflowPipelineOptions).setLabels(expected);
  }
}
