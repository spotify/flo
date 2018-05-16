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
import static org.mockito.Mockito.when;

import com.spotify.flo.context.TerminationHook;
import com.typesafe.config.Config;
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

  @Mock
  private Config config;

  private TerminationHook terminationHook;

  @Before
  public void setUp() {
    when(config.getString("styx.component.id")).thenReturn("foo");
    when(config.getString("styx.workflow.id")).thenReturn("bar");
    when(config.getString("styx.parameter")).thenReturn("2018-01-01");
    when(config.getString("styx.execution.id")).thenReturn("foobar");

    terminationHook = new TerminationLogging(config);
  }

  @Test
  public void shouldWriteToTerminationLog() throws IOException {
    final Path tempFile = Files.createTempFile("termination-log-", "");
    tempFile.toFile().deleteOnExit();

    when(config.getString("styx.termination.log")).thenReturn(tempFile.toString());

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
    when(config.getString("styx.termination.log")).thenReturn(".");
    terminationHook.accept(20);
  }
}
