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

import java.util.Objects;
import java.util.function.Supplier;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;

public class LabeledPipelineOptionsSupplier implements Supplier<DataflowPipelineOptions> {

  private static final String DEFAULT_LABEL_PREFIX = "spotify";

  private static final String STYX_COMPONENT_ID_LABEL = "styx-component-id";
  private static final String STYX_WORKFLOW_ID_LABEL = "styx-workflow-id";
  private static final String STYX_PARAMETER_LABEL = "styx-parameter";
  private static final String STYX_EXECUTION_ID_LABEL = "styx-execution-id";
  private static final String STYX_TRIGGER_ID_LABEL = "styx-trigger-id";
  private static final String STYX_TRIGGER_TYPE_LABEL = "styx-trigger-type";

  private String labelPrefix;

  private Supplier<DataflowPipelineOptions> supplier;

  private LabeledPipelineOptionsSupplier(Supplier<DataflowPipelineOptions> supplier) {
    this(DEFAULT_LABEL_PREFIX, supplier);
  }
  
  private LabeledPipelineOptionsSupplier(String labelPrefix,
                                         Supplier<DataflowPipelineOptions> supplier) {
    this.labelPrefix = Objects.requireNonNull(labelPrefix);
    this.supplier = Objects.requireNonNull(supplier);
  }

  public DataflowPipelineOptions get() {
    final DataflowPipelineOptions pipelineOptions = supplier.get();
    // TODO: add labels
    pipelineOptions.setLabels(null);
    return pipelineOptions;
  }

  public static Supplier<DataflowPipelineOptions> from(Supplier<DataflowPipelineOptions> supplier) {
    return new LabeledPipelineOptionsSupplier(supplier);
  }

  public static Supplier<DataflowPipelineOptions> from(String labelPrefix,
                                                       Supplier<DataflowPipelineOptions> supplier) {
    return new LabeledPipelineOptionsSupplier(labelPrefix, supplier);
  }


//  @Override
//  default PipelineOptions options() {
//    DataflowPipelineOptions pipelineOptions = PipelineOptionsSupplier.super.options()
//        .as(DataflowPipelineOptions.class);
//
//    pipelineOptions.setLabels(getLabelsMap());
//    return pipelineOptions;
//  }
//
//  default Map<String,String> getLabelsMap(){
//    final Config config = ConfigFactory.load();
//
//    final Map<String, String> labels = new HashMap<>();
//
//    putLabel(labels, STYX_COMPONENT_ID_LABEL, config.getString(Constants.STYX_COMPONENT_ID));
//
////    labels.put(STYX_COMPONENT_ID_LABEL, config.getString(Constants.STYX_COMPONENT_ID));
////    labels.put(STYX_WORKFLOW_ID_LABEL, config.getString(Constants.STYX_WORKFLOW_ID));
////    labels.put(STYX_PARAMETER_LABEL, config.getString(Constants.STYX_PARAMETER));
////    labels.put(LABEL_PREFIX+STYX_EXECUTION_ID_LABEL, config.getString(Constants.STYX_EXECUTION_ID));
//  }
//
//  default void putLabel(Map<String, String> labels, String label, String value) {
//    //sanitize
//    //validate
////    labels.put(LABEL_PREFIX + label, value);
//  }
//
//  default String sanitizeLabelValue(String pad, String value) {
//    return "new-value";
//  }

}
