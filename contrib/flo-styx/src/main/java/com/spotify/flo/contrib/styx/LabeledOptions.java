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

import com.spotify.flo.contrib.beam.BeamOptions;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.awt.Container;
import java.util.HashMap;
import java.util.Map;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.options.PipelineOptions;

public interface LabeledOptions extends BeamOptions {

  public static final String DEFAULT_LABEL_PREFIX = "";
  public static final String STYX_COMPONENT_ID_LABEL = "styx-component-id";
  public static final String STYX_WORKFLOW_ID_LABEL = "styx-workflow-id";
  public static final String STYX_PARAMETER_LABEL = "styx-parameter";
  public static final String STYX_EXECUTION_ID_LABEL = "styx-execution-id";
  public static final String STYX_TRIGGER_ID_LABEL = "styx-trigger-id";
  public static final String STYX_TRIGGER_TYPE_LABEL = "styx-trigger-type";


  String labelPrefix();

  String componentId();

  String workflowId();

  String parameter();

  String executionId();

  String triggerId();

  String triggerType();

  @Override
  default PipelineOptions options() {
    DataflowPipelineOptions pipelineOptions = BeamOptions.super.options()
        .as(DataflowPipelineOptions.class);

    pipelineOptions.setLabels(getLabelsMap());
    return pipelineOptions;
  }

  default Map<String,String> getLabelsMap(){
    final Config config = ConfigFactory.load();

    final Map<String, String> labels = new HashMap<>();

    putLabel(labels, STYX_COMPONENT_ID_LABEL, config.getString(Constants.STYX_COMPONENT_ID));

//    labels.put(STYX_COMPONENT_ID_LABEL, config.getString(Constants.STYX_COMPONENT_ID));
//    labels.put(STYX_WORKFLOW_ID_LABEL, config.getString(Constants.STYX_WORKFLOW_ID));
//    labels.put(STYX_PARAMETER_LABEL, config.getString(Constants.STYX_PARAMETER));
//    labels.put(LABEL_PREFIX+STYX_EXECUTION_ID_LABEL, config.getString(Constants.STYX_EXECUTION_ID));
  }

  default void putLabel(Map<String, String> labels, String label, String value) {
    //sanitize
    //validate
//    labels.put(LABEL_PREFIX + label, value);
  }

  default String sanitizeLabelValue(String pad, String value) {
    return "new-value";
  }

}
