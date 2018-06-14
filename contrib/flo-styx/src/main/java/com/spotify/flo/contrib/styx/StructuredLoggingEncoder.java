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

import static java.nio.charset.StandardCharsets.UTF_8;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.IThrowableProxy;
import ch.qos.logback.classic.spi.StackTraceElementProxy;
import ch.qos.logback.core.encoder.Encoder;
import ch.qos.logback.core.encoder.EncoderBase;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.spotify.flo.TaskId;
import com.spotify.flo.Tracing;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.time.Instant;
import java.util.Formatter;
import java.util.Map;
import java.util.Optional;

/**
 * A log message {@link Encoder} that emits log messages decorated with Styx and Flo metadata.
 * If running under Styx, log messages are encoded as Stackdriver structured json and plain
 * text otherwise for easy reading when developing and running locally.
 */
public class StructuredLoggingEncoder extends EncoderBase<ILoggingEvent> {

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final ObjectWriter WRITER = MAPPER.writer();
  private static final String LINE_SEPARATOR = System.getProperty("line.separator");
  private static final byte[] LINE_SEPARATOR_BYTES = LINE_SEPARATOR.getBytes();

  private final boolean isStyxExecution = System.getenv().containsKey("STYX_EXECUTION_ID");
  private final WorkflowBuilder template = createTemplate();

  private final TaskId envTaskId;

  public StructuredLoggingEncoder() {
    this.envTaskId = Optional.ofNullable(System.getenv("FLO_TASK_ID"))
        .map(TaskId::parse)
        .orElse(null);
  }

  @Override
  public byte[] encode(ILoggingEvent event) {
    if (isStyxExecution) {
      return encodeStructured(event);
    } else {
      return encodeText(event);
    }
  }

  private byte[] encodeStructured(ILoggingEvent event) {
    final StructuredLogMessageBuilder structuredLogMessageBuilder =
        StructuredLogMessage.newBuilder();

    // Standard fields
    structuredLogMessageBuilder.time(Instant.ofEpochMilli(event.getTimeStamp()).toString());
    structuredLogMessageBuilder.severity(event.getLevel().toString());
    structuredLogMessageBuilder.logger(event.getLoggerName());
    structuredLogMessageBuilder.thread(event.getThreadName());
    final StringBuilder message = new StringBuilder(event.getFormattedMessage());
    final IThrowableProxy t = event.getThrowableProxy();
    if (t != null) {
      message.append('\n');
      writeStack(message, t, "", 0, "\n");
    }
    structuredLogMessageBuilder.message(message.toString());

    // Workflow metadata
    final WorkflowBuilder workflowBuilder = WorkflowBuilder.from(template);
    final TaskId taskId = taskId();
    workflowBuilder.task_id(taskId != null ? taskId.toString() : "");
    workflowBuilder.task_name(taskId != null ? taskId.name() : "");
    workflowBuilder.task_args(taskId != null ? taskId.args() : "");

    // Serialize to json
    try (final ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
      WRITER.writeValue(baos, structuredLogMessageBuilder.workflow(workflowBuilder.build()).build());
      baos.write(LINE_SEPARATOR_BYTES);
      return baos.toByteArray();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private TaskId taskId() {
    return envTaskId != null ? envTaskId : Tracing.TASK_ID.get();
  }

  private byte[] encodeText(ILoggingEvent event) {
    final TaskId taskId = taskId();
    final String taskIdString = (taskId != null) ? taskId.toString() : "";
    final StringBuilder sb = new StringBuilder();
    final Formatter formatter = new Formatter(sb);
    formatter.format("%s %-5s [%s] %s: %s%n",
        Instant.ofEpochMilli(event.getTimeStamp()).toString(),
        event.getLevel(), taskIdString, loggerName(event), event.getFormattedMessage());
    final IThrowableProxy t = event.getThrowableProxy();
    if (t != null) {
      writeStack(sb, t, "", 0, LINE_SEPARATOR);
    }
    return sb.toString().getBytes(UTF_8);
  }

  private String loggerName(ILoggingEvent event) {
    final String loggerName = event.getLoggerName();
    final int ix = loggerName.lastIndexOf('.');
    if (ix == -1) {
      return loggerName;
    } else {
      return loggerName.substring(ix + 1);
    }
  }

  private static void writeStack(StringBuilder sb, IThrowableProxy t, String caption, int indent, String linebreak) {
    if (t == null) {
      return;
    }
    indent(sb, indent).append(caption).append(t.getClassName()).append(": ").append(t.getMessage()).append(linebreak);
    final StackTraceElementProxy[] trace = t.getStackTraceElementProxyArray();
    if (trace != null) {
      int commonFrames = t.getCommonFrames();
      int printFrames = trace.length - commonFrames;
      for (int i = 0; i < printFrames; i++) {
        indent(sb, indent).append("\t").append(trace[i]).append(linebreak);
      }
      if (commonFrames != 0) {
        indent(sb, indent).append("\t... ").append(commonFrames).append(" more").append(linebreak);
      }
      final IThrowableProxy[] suppressed = t.getSuppressed();
      for (IThrowableProxy st : suppressed) {
        writeStack(sb, st, "Suppressed: ", indent + 1, linebreak);
      }
    }

    writeStack(sb, t.getCause(), "Caused by: ", indent, linebreak);
  }

  private static StringBuilder indent(StringBuilder sb, int indent) {
    for (int j = 0; j < indent; ++j) {
      sb.append('\t');
    }
    return sb;
  }

  @Override
  public byte[] headerBytes() {
    return null;
  }

  @Override
  public byte[] footerBytes() {
    return null;
  }

  private static WorkflowBuilder createTemplate() {
    final Map<String, String> env = System.getenv();
    return StructuredLogMessage.Workflow.newBuilder()
        .styx_component_id(env.getOrDefault("STYX_COMPONENT_ID", ""))
        .styx_workflow_id(env.getOrDefault("STYX_WORKFLOW_ID", ""))
        .styx_docker_args(env.getOrDefault("STYX_DOCKER_ARGS", ""))
        .styx_docker_image(env.getOrDefault("STYX_DOCKER_IMAGE", ""))
        .styx_commit_sha(env.getOrDefault("STYX_COMMIT_SHA", ""))
        .styx_parameter(env.getOrDefault("STYX_PARAMETER", ""))
        .styx_execution_id(env.getOrDefault("STYX_EXECUTION_ID", ""))
        .styx_trigger_id(env.getOrDefault("STYX_TRIGGER_ID", ""))
        .styx_trigger_type(env.getOrDefault("STYX_TRIGGER_TYPE", ""));
  }
}
