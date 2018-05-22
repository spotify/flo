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


import static com.spotify.flo.contrib.styx.Constants.STYX_COMPONENT_ID;
import static com.spotify.flo.contrib.styx.Constants.STYX_EXECUTION_ID;
import static com.spotify.flo.contrib.styx.Constants.STYX_PARAMETER;
import static com.spotify.flo.contrib.styx.Constants.STYX_TRIGGER_ID;
import static com.spotify.flo.contrib.styx.Constants.STYX_TRIGGER_TYPE;
import static com.spotify.flo.contrib.styx.Constants.STYX_WORKFLOW_ID;

import com.typesafe.config.Config;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

final class LabelUtil {

  private static final String STYX_COMPONENT_ID_LABEL_KEY = "styx-component-id";
  private static final String STYX_WORKFLOW_ID_LABEL_KEY = "styx-workflow-id";
  private static final String STYX_PARAMETER_LABEL_KEY = "styx-parameter";
  private static final String STYX_EXECUTION_ID_LABEL_KEY = "styx-execution-id";
  private static final String STYX_TRIGGER_ID_LABEL_KEY = "styx-trigger-id";
  private static final String STYX_TRIGGER_TYPE_LABEL_KEY = "styx-trigger-type";

  private static final int LABEL_KEY_MAX_LENGTH = 63;
  private static final int LABEL_VALUE_MAX_LENGTH = 63;
  private static final String LABEL_KEY_REGEX = "^[a-z]([-a-z0-9]*[a-z0-9])?$";
  private static final String LABEL_VALUE_REGEX = "^([a-z]([-a-z0-9]*[a-z0-9])?)?$";
  private static final String LABEL_VALUE_REPLACE_REGEX = "[^-a-z0-9]";

  private LabelUtil() {
    throw new UnsupportedOperationException();
  }

  static Map<String, String> buildLabels(String labelPrefix, Config config) {
    final Map<String, String> map = new HashMap<>();

    final String componentIdLabelKey = validateAndPrefixLabelKey(labelPrefix,
        STYX_COMPONENT_ID_LABEL_KEY);
    final String workflowIdLabelKey = validateAndPrefixLabelKey(labelPrefix,
        STYX_WORKFLOW_ID_LABEL_KEY);
    final String parameterLabelKey = validateAndPrefixLabelKey(labelPrefix,
        STYX_PARAMETER_LABEL_KEY);
    final String executionIdLabelKey = validateAndPrefixLabelKey(labelPrefix,
        STYX_EXECUTION_ID_LABEL_KEY);
    final String triggerIdLabelKey = validateAndPrefixLabelKey(labelPrefix,
        STYX_TRIGGER_ID_LABEL_KEY);
    final String triggerTypeLabelKey = validateAndPrefixLabelKey(labelPrefix,
        STYX_TRIGGER_TYPE_LABEL_KEY);
    
    sanitizeLabelValue("c", STYX_COMPONENT_ID, config)
        .ifPresent(value -> map.put(componentIdLabelKey, value));
    sanitizeLabelValue("w", STYX_WORKFLOW_ID, config)
        .ifPresent(value -> map.put(workflowIdLabelKey, value));
    sanitizeLabelValue("p", STYX_PARAMETER, config)
        .ifPresent(value -> map.put(parameterLabelKey, value));
    sanitizeLabelValue("e", STYX_EXECUTION_ID, config)
        .ifPresent(value -> map.put(executionIdLabelKey, value));
    sanitizeLabelValue("t", STYX_TRIGGER_ID, config)
        .ifPresent(value -> map.put(triggerIdLabelKey, value));
    sanitizeLabelValue("tt", STYX_TRIGGER_TYPE, config)
        .ifPresent(value -> map.put(triggerTypeLabelKey, value));

    return map;
  }

  private static String validateAndPrefixLabelKey(String labelPrefix, String label) {
    final String prefixedLabelKey = String.format("%s-%s", labelPrefix, label);

    if (prefixedLabelKey.length() > LABEL_KEY_MAX_LENGTH) {
      throw new IllegalArgumentException("Invalid label key: Too long, must be <= 63 characters: "
                                         + prefixedLabelKey);
    }
    
    if (!prefixedLabelKey.matches(LABEL_KEY_REGEX)) {
      throw new IllegalArgumentException("Invalid label key: " + prefixedLabelKey);
    }
    
    return prefixedLabelKey;
  }

  private static Optional<String> sanitizeLabelValue(String pad, String path, Config config) {
    if (!config.hasPath(path)) {
      return Optional.empty();
    }

    // Only allow [-a-z0-9]
    String value = config.getString(path).toLowerCase().replaceAll(LABEL_VALUE_REPLACE_REGEX, "-");

    // First character must be [a-z]
    if (!Character.isAlphabetic(value.charAt(0))) {
      value = String.format("%s-%s", pad, value);
    }

    // We always assume padding at the end is needed to be on the safe side
    // so the worst case is we trim more than we actually need
    if (value.length() + pad.length() > LABEL_VALUE_MAX_LENGTH) {
      value = value.substring(0, LABEL_VALUE_MAX_LENGTH - pad.length());
    }

    // Last character must be [a-z0-9] (not -)
    if (value.charAt(value.length() - 1) == '-') {
      value = String.format("%s%s", value, pad);
    }

    // Final check
    if (!value.matches(LABEL_VALUE_REGEX)) {
      return Optional.empty();
    }

    return Optional.of(value);
  }
}
