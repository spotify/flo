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

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.spotify.flo.context.TerminationHook;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class TerminationLoggingTest {

  private static final String STYX_TERMINATION_LOG = "styx.termination.log";
  private static final String STYX_COMPONENT_ID = "styx.component.id";
  private static final String STYX_WORKFLOW_ID = "styx.workflow.id";
  private static final String STYX_PARAMETER = "styx.parameter";
  private static final String STYX_EXECUTION_ID = "styx.execution.id";

  @Mock
  private Config config;

  private TerminationHook terminationHook;

  @Before
  public void setUp() {
    when(config.hasPath(STYX_TERMINATION_LOG)).thenReturn(true);
    when(config.getString(STYX_COMPONENT_ID)).thenReturn("foo");
    when(config.getString(STYX_WORKFLOW_ID)).thenReturn("bar");
    when(config.getString(STYX_PARAMETER)).thenReturn("2018-01-01");
    when(config.getString(STYX_EXECUTION_ID)).thenReturn("foobar");

    terminationHook = new TerminationLogging(config);
  }

  @Test
  public void shouldLoadConfig() {
    final Config loadedConfig = ConfigFactory.load();
    assertThat(loadedConfig.hasPath(STYX_TERMINATION_LOG), is(false));
    assertThat(loadedConfig.getString(STYX_COMPONENT_ID), is("UNKNOWN_COMPONENT"));
    assertThat(loadedConfig.getString(STYX_WORKFLOW_ID), is("UNKNOWN_WORKFLOW"));
    assertThat(loadedConfig.getString(STYX_PARAMETER), is("UNKNOWN_PARAMETER"));
    assertThat(loadedConfig.getString(STYX_EXECUTION_ID), is("UNKNOWN_EXECUTION"));
  }

  @Test
  public void shouldDoNothingIfNoTerminationLog() {
    when(config.hasPath(STYX_TERMINATION_LOG)).thenReturn(false);
    verify(config, never()).getString(anyString());
  }

  @Test
  public void shouldWriteToTerminationLog() throws IOException {
    final Path tempFile = Files.createTempFile("termination-log-", "");
    tempFile.toFile().deleteOnExit();

    when(config.getString(STYX_TERMINATION_LOG)).thenReturn(tempFile.toString());

    final String expected = "{\"component_id\": \"foo\","
                            + "\"workflow_id\": \"bar\","
                            + "\"parameter\": \"2018-01-01\","
                            + "\"execution_id\": \"foobar\","
                            + "\"event\": \"exited\","
                            + "\"exit_code\": 20}";

    terminationHook.accept(20);

    final String content = new String(Files.readAllBytes(tempFile));
    assertThat(content, is(expected));
  }
  
  @Test(expected = RuntimeException.class)
  public void shouldFailToWriteTerminationLog() {
    when(config.getString(STYX_TERMINATION_LOG)).thenReturn(".");
    terminationHook.accept(20);
  }
}
