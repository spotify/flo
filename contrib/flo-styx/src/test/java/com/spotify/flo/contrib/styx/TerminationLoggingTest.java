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

import com.spotify.flo.context.TerminationHook;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.EnvironmentVariables;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class TerminationLoggingTest {

  private static final String STYX_TERMINATION_LOG = "STYX_TERMINATION_LOG";
  private static final String STYX_COMPONENT_ID = "STYX_COMPONENT_ID";
  private static final String STYX_WORKFLOW_ID = "STYX_WORKFLOW_ID";
  private static final String STYX_PARAMETER = "STYX_PARAMETER";
  private static final String STYX_EXECUTION_ID = "STYX_EXECUTION_ID";

  @Rule
  public final EnvironmentVariables environmentVariables = new EnvironmentVariables();

  private TerminationHook terminationHook;

  @Before
  public void setUp() {
    environmentVariables.set(STYX_COMPONENT_ID, "foo");
    environmentVariables.set(STYX_WORKFLOW_ID, "bar");
    environmentVariables.set(STYX_PARAMETER, "2018-01-01");
    environmentVariables.set(STYX_EXECUTION_ID, "foobar");

    terminationHook = new TerminationLogging();
  }

  @Test
  public void shouldNotWriteFileIfNoTerminationLog() throws IOException {
    final Path tempFile = Files.createTempFile("termination-log-", "");
    tempFile.toFile().deleteOnExit();

    terminationHook.accept(20);

    final String content = new String(Files.readAllBytes(tempFile));
    assertThat(content, is(""));
  }

  @Test
  public void shouldWriteToTerminationLog() throws IOException {
    final Path tempFile = Files.createTempFile("termination-log-", "");
    tempFile.toFile().deleteOnExit();

    environmentVariables.set(STYX_TERMINATION_LOG, tempFile.toString());

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

  @Test
  public void shouldWriteToTerminationLogWithDefaultValues() throws IOException {
    final Path tempFile = Files.createTempFile("termination-log-", "");
    tempFile.toFile().deleteOnExit();

    environmentVariables.set(STYX_TERMINATION_LOG, tempFile.toString());
    environmentVariables
        .clear(STYX_COMPONENT_ID, STYX_WORKFLOW_ID, STYX_PARAMETER, STYX_EXECUTION_ID);

    final String expected = "{\"component_id\": \"UNKNOWN_COMPONENT_ID\","
                            + "\"workflow_id\": \"UNKNOWN_WORKFLOW_ID\","
                            + "\"parameter\": \"UNKNOWN_PARAMETER\","
                            + "\"execution_id\": \"UNKNOWN_EXECUTION_ID\","
                            + "\"event\": \"exited\","
                            + "\"exit_code\": 20}";

    terminationHook.accept(20);

    final String content = new String(Files.readAllBytes(tempFile));
    assertThat(content, is(expected));
  }
  
  @Test(expected = RuntimeException.class)
  public void shouldFailToWriteTerminationLog() {
    environmentVariables.set(STYX_TERMINATION_LOG, ".");
    terminationHook.accept(20);
  }
}
