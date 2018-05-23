/*-
 * -\-\-
 * flo-styx
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

import com.google.common.annotations.VisibleForTesting;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Sanitized Styx environment variables where key should match "^([a-z]([-a-z0-9]*[a-z0-9])?)?$" and
 * less than 64 characters, and value should match "^([a-z]([-a-z0-9]*[a-z0-9])?)?$" and less than
 * 64 characters.
 */
public final class Environment {

  private static final String DEFAULT_KEY_PREFIX = "spotify";

  private static final String STYX_COMPONENT_ID = "styx.component.id";
  private static final String STYX_WORKFLOW_ID = "styx.workflow.id";
  private static final String STYX_PARAMETER = "styx.parameter";
  private static final String STYX_EXECUTION_ID = "styx.execution.id";
  private static final String STYX_TRIGGER_ID = "styx.trigger.id";
  private static final String STYX_TRIGGER_TYPE = "styx.trigger.type";

  private static final String STYX_COMPONENT_ID_KEY = "styx-component-id";
  private static final String STYX_WORKFLOW_ID_KEY = "styx-workflow-id";
  private static final String STYX_PARAMETER_KEY = "styx-parameter";
  private static final String STYX_EXECUTION_ID_KEY = "styx-execution-id";
  private static final String STYX_TRIGGER_ID_KEY = "styx-trigger-id";
  private static final String STYX_TRIGGER_TYPE_KEY = "styx-trigger-type";

  private static final int KEY_MAX_LENGTH = 63;
  private static final int VALUE_MAX_LENGTH = 63;
  private static final String KEY_REGEX = "^[a-z]([-a-z0-9]*[a-z0-9])?$";
  private static final String VALUE_REGEX = "^([a-z]([-a-z0-9]*[a-z0-9])?)?$";
  private static final String VALUE_REPLACE_REGEX = "[^-a-z0-9]";

  private Environment() {
    throw new UnsupportedOperationException();
  }

  /**
   * Get sanitized Styx environment variables which are visible to containers launched by Styx:
   *
   * <ul>
   * <li>spotify-styx-component-id</li>
   * <li>spotify-styx-workflow-id</li>
   * <li>spotify-styx-parameter</li>
   * <li>spotify-styx-trigger-id</li>
   * <li>spotify-styx-trigger-type</li>
   * </ul>
   *
   * @return Styx environment variables which are visible to containers launched by Styx
   */
  public static Map<String, String> getEnv() {
    return getEnv(DEFAULT_KEY_PREFIX);
  }

  /**
   * Get sanitized Styx environment variables which are visible to containers launched by Styx:
   *
   * <ul>
   * <li>${keyPrefix}-styx-component-id</li>
   * <li>${keyPrefix}-styx-workflow-id</li>
   * <li>${keyPrefix}-styx-parameter</li>
   * <li>${keyPrefix}-styx-trigger-id</li>
   * <li>${keyPrefix}-styx-trigger-type</li>
   * </ul>
   *
   * @param keyPrefix prefix added to the key
   *
   * @return Styx environment variables which are visible to containers launched by Styx
   */
  public static Map<String, String> getEnv(String keyPrefix) {
    return getEnv(keyPrefix, ConfigFactory.load());
  }

  @VisibleForTesting
  static Map<String, String> getEnv(String keyPrefix, Config config) {
    final Map<String, String> map = new HashMap<>();

    final String componentIdKey = validateAndPrefixKey(keyPrefix, STYX_COMPONENT_ID_KEY);
    final String workflowIdKey = validateAndPrefixKey(keyPrefix, STYX_WORKFLOW_ID_KEY);
    final String parameterKey = validateAndPrefixKey(keyPrefix, STYX_PARAMETER_KEY);
    final String executionIdKey = validateAndPrefixKey(keyPrefix, STYX_EXECUTION_ID_KEY);
    final String triggerIdKey = validateAndPrefixKey(keyPrefix, STYX_TRIGGER_ID_KEY);
    final String triggerTypeKey = validateAndPrefixKey(keyPrefix, STYX_TRIGGER_TYPE_KEY);

    sanitizeValue("c", STYX_COMPONENT_ID, config)
        .ifPresent(value -> map.put(componentIdKey, value));
    sanitizeValue("w", STYX_WORKFLOW_ID, config)
        .ifPresent(value -> map.put(workflowIdKey, value));
    sanitizeValue("p", STYX_PARAMETER, config)
        .ifPresent(value -> map.put(parameterKey, value));
    sanitizeValue("e", STYX_EXECUTION_ID, config)
        .ifPresent(value -> map.put(executionIdKey, value));
    sanitizeValue("t", STYX_TRIGGER_ID, config)
        .ifPresent(value -> map.put(triggerIdKey, value));
    sanitizeValue("tt", STYX_TRIGGER_TYPE, config)
        .ifPresent(value -> map.put(triggerTypeKey, value));

    return map;
  }

  private static String validateAndPrefixKey(String keyPrefix, String key) {
    final String prefixedLabelKey = String.format("%s-%s", keyPrefix, key);

    if (prefixedLabelKey.length() > KEY_MAX_LENGTH) {
      throw new IllegalArgumentException("Invalid key: Too long, must be <= 63 characters: "
                                         + prefixedLabelKey);
    }

    if (!prefixedLabelKey.matches(KEY_REGEX)) {
      throw new IllegalArgumentException("Invalid key: " + prefixedLabelKey);
    }

    return prefixedLabelKey;
  }

  private static Optional<String> sanitizeValue(String pad, String path, Config config) {
    if (!config.hasPath(path)) {
      return Optional.empty();
    }

    // Only allow [-a-z0-9]
    String value = config.getString(path).toLowerCase().replaceAll(VALUE_REPLACE_REGEX, "-");

    // First character must be [a-z]
    if (!Character.isAlphabetic(value.charAt(0))) {
      value = String.format("%s-%s", pad, value);
    }

    // We always assume padding at the end is needed to be on the safe side
    // so the worst case is we trim more than we actually need
    if (value.length() + pad.length() > VALUE_MAX_LENGTH) {
      value = value.substring(0, VALUE_MAX_LENGTH - pad.length());
    }

    // Last character must be [a-z0-9] (not -)
    if (value.charAt(value.length() - 1) == '-') {
      value = String.format("%s%s", value, pad);
    }

    // Final check
    if (!value.matches(VALUE_REGEX)) {
      return Optional.empty();
    }

    return Optional.of(value);
  }
}
