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

import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import ch.qos.logback.core.joran.spi.JoranException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.spotify.flo.Task;
import com.spotify.flo.context.FloRunner;
import io.norberg.automatter.jackson.AutoMatterModule;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.EnvironmentVariables;
import org.junit.contrib.java.lang.system.SystemErrRule;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

@RunWith(JUnitParamsRunner.class)
public class StructuredLoggingTest {

  private static final ObjectMapper MAPPER = new ObjectMapper()
      .registerModule(new AutoMatterModule());

  @Rule public final SystemErrRule stderr = new SystemErrRule().enableLog();
  @Rule public final EnvironmentVariables env = new EnvironmentVariables();

  @Before
  public void setUp() {
    stderr.clearLog();
    env.set("FLO_LOGGING_LEVEL", "DEBUG");
  }

  private void configureLogbackFromFile() {
    final ch.qos.logback.classic.Logger rootLogger = (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(
        ch.qos.logback.classic.Logger.ROOT_LOGGER_NAME);

    final LoggerContext context = rootLogger.getLoggerContext();
    context.reset();

    final JoranConfigurator configurator = new JoranConfigurator();
    configurator.setContext(context);
    try {
      configurator.doConfigure(getClass().getResource("/logback.xml"));
    } catch (JoranException e) {
      throw new RuntimeException(e);
    }
  }

  @Parameters({"true", "false"})
  @Test
  public void testStructuredLoggingProgrammaticSetup(boolean forking) throws Exception {
    configureForking(forking);
    verifyStructuredLogging(() -> StructuredLogging.configureStructuredLogging(Level.DEBUG));
  }

  @Parameters({"true", "false"})
  @Test
  public void testStructuredLoggingFileSetup(boolean forking) throws Exception {
    configureForking(forking);
    verifyStructuredLogging(this::configureLogbackFromFile);
  }

  private void verifyStructuredLogging(Runnable setupLogging) throws Exception {
    final String styx_component_id = "test_component_id-" + UUID.randomUUID();
    final String styx_workflow_id = "test_workflow_id-" + UUID.randomUUID();
    final String styx_docker_args = "test_docker_args-" + UUID.randomUUID();
    final String styx_docker_image = "test_docker_image-" + UUID.randomUUID();
    final String styx_commit_sha = "test_commit_sha-" + UUID.randomUUID();
    final String styx_parameter = "test_parameter-" + UUID.randomUUID();
    final String styx_execution_id = "test_execution_id-" + UUID.randomUUID();
    final String styx_trigger_id = "test_trigger_id-" + UUID.randomUUID();
    final String styx_trigger_type = "test_trigger_type-" + UUID.randomUUID();

    env.set("STYX_COMPONENT_ID", styx_component_id);
    env.set("STYX_WORKFLOW_ID", styx_workflow_id);
    env.set("STYX_DOCKER_ARGS", styx_docker_args);
    env.set("STYX_DOCKER_IMAGE", styx_docker_image);
    env.set("STYX_COMMIT_SHA", styx_commit_sha);
    env.set("STYX_PARAMETER", styx_parameter);
    env.set("STYX_EXECUTION_ID", styx_execution_id);
    env.set("STYX_TRIGGER_ID", styx_trigger_id);
    env.set("STYX_TRIGGER_TYPE", styx_trigger_type);
    env.set("STYX_LOGGING", "structured");

    setupLogging.run();

    final String infoMessageText = "hello world " + UUID.randomUUID();
    final String errorMessageText = "danger danger! " + UUID.randomUUID();

    final Task<String> task = loggingTask(infoMessageText, errorMessageText);

    final String exceptionStackTrace = FloRunner.runTask(task)
        .future().get(30, TimeUnit.SECONDS);

    // Wait for log messages to flush
    Thread.sleep(1000);

    final String output = stderr.getLog();
    final List<StructuredLogMessage> messages = MAPPER.readerFor(StructuredLogMessage.class)
        .<StructuredLogMessage>readValues(output)
        .readAll();

    final StructuredLogMessage infoMessage = messages.stream()
        .filter(m -> infoMessageText.equals(m.message()))
        .findFirst().orElseThrow(AssertionError::new);

    final String errorMessageWithException = errorMessageText + "\n" + exceptionStackTrace;
    final StructuredLogMessage errorMessage = messages.stream()
        .filter(m -> m.message().equals(errorMessageWithException))
        .findFirst().orElseThrow(AssertionError::new);

    assertThat(infoMessage.severity(), is("INFO"));
    assertThat(errorMessage.severity(), is("ERROR"));

    for (StructuredLogMessage message : new StructuredLogMessage[]{infoMessage, errorMessage}) {
      assertThat((double) Instant.parse(message.time()).toEpochMilli(),
          is(closeTo(Instant.now().toEpochMilli(), 30_000)));
      assertThat(message.workflow().framework(), is("flo"));
      assertThat(message.workflow().styx().component_id(), is(styx_component_id));
      assertThat(message.workflow().styx().workflow_id(), is(styx_workflow_id));
      assertThat(message.workflow().styx().docker_args(), is(styx_docker_args));
      assertThat(message.workflow().styx().docker_image(), is(styx_docker_image));
      assertThat(message.workflow().styx().commit_sha(), is(styx_commit_sha));
      assertThat(message.workflow().styx().parameter(), is(styx_parameter));
      assertThat(message.workflow().styx().execution_id(), is(styx_execution_id));
      assertThat(message.workflow().styx().trigger_id(), is(styx_trigger_id));
      assertThat(message.workflow().styx().trigger_type(), is(styx_trigger_type));
      assertThat(message.workflow().task().id(), is(task.id().toString()));
      assertThat(message.workflow().task().name(), is(task.id().name()));
      assertThat(message.workflow().task().args(), is(task.id().args()));
    }
  }

  @Parameters({"true", "false"})
  @Test
  public void testTextLoggingProgrammaticSetup(boolean forking) throws Exception {
    configureForking(forking);
    StructuredLogging.configureStructuredLogging();
    verifyTextLogging();
  }

  @Parameters({"true", "false"})
  @Test
  public void testTextLoggingFileSetup(boolean forking) throws Exception {
    configureForking(forking);
    configureLogbackFromFile();
    verifyTextLogging();
  }

  @Test
  public void testStructuredLoggingWithNoMetadata() throws IOException {
    final String executionId = UUID.randomUUID().toString();
    env.set("STYX_EXECUTION_ID", executionId);

    // Trigger structured logging
    env.set("STYX_LOGGING", "structured");

    configureLogbackFromFile();
    final Logger logger = LoggerFactory.getLogger("test");
    logger.info("hello world!");
    final String output = stderr.getLog();

    final StructuredLogMessage message = MAPPER.readValue(output, StructuredLogMessage.class);

    assertThat(message.message(), is("hello world!"));

    assertThat(message.workflow().styx().execution_id(), is(executionId));
    assertThat(message.workflow().styx().component_id(), is(""));
    assertThat(message.workflow().styx().workflow_id(), is(""));
    assertThat(message.workflow().styx().docker_args(), is(""));
    assertThat(message.workflow().styx().docker_image(), is(""));
    assertThat(message.workflow().styx().commit_sha(), is(""));
    assertThat(message.workflow().styx().parameter(), is(""));
    assertThat(message.workflow().styx().trigger_id(), is(""));
    assertThat(message.workflow().styx().trigger_type(), is(""));
    assertThat(message.workflow().framework(), is("flo"));
    assertThat(message.workflow().task().id(), is(""));
    assertThat(message.workflow().task().name(), is(""));
    assertThat(message.workflow().task().args(), is(""));
  }

  private void verifyTextLogging() throws Exception {

    final String infoMessageText = "hello world " + UUID.randomUUID();
    final String errorMessageText = "danger danger! " + UUID.randomUUID();

    final Task<String> task = loggingTask(infoMessageText, errorMessageText);
    final String exceptionStackTrace = FloRunner.runTask(task)
        .future().get(30, TimeUnit.SECONDS);

    // Wait for log messages to flush
    Thread.sleep(1000);

    final String output = stderr.getLog();
    final String lineSeparator = System.getProperty("line.separator");
    final List<String> lines = Arrays.asList(output.split(lineSeparator));

    lines.stream()
        .filter(s -> s.contains("[" + task.id() + "]"))
        .filter(s -> s.contains("INFO"))
        .filter(s -> s.contains(infoMessageText))
        .findAny().orElseThrow(AssertionError::new);

    lines.stream()
        .filter(s -> s.contains("[" + task.id() + "]"))
        .filter(s -> s.contains("ERROR"))
        .filter(s -> s.contains(errorMessageText))
        .findAny().orElseThrow(AssertionError::new);
    assertThat(output, containsString(exceptionStackTrace));
  }

  private Task<String> loggingTask(String infoMessage, String errorMessage) {
    return Task.named("test", UUID.randomUUID().toString(), 4711).ofType(String.class)
        .process(() -> {
          final Logger logger = LoggerFactory.getLogger(StructuredLoggingTest.class);
          logger.info(infoMessage);
          final Exception exception = exception();
          logger.error(errorMessage, exception);
          return stackTrace(exception);
        });
  }

  private static String stackTrace(Throwable t) {
    final StringWriter sw = new StringWriter();
    final PrintWriter pw = new PrintWriter(sw);
    t.printStackTrace(pw);
    pw.flush();
    return sw.toString();
  }

  private static Exception exception() {
    final Exception exception = new Exception("foo!", new IOException("bar!"));
    exception.addSuppressed(new RuntimeException("suppressed1", new IllegalStateException("inner suppressed1")));
    exception.addSuppressed(new RuntimeException("suppressed2", new IllegalStateException("inner suppressed2")));
    return exception;
  }

  private void configureForking(boolean forking) {
    env.set("FLO_FORKING", Boolean.toString(forking));
  }
}
