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

import static ch.qos.logback.classic.Level.fromLocationAwareLoggerInteger;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.ConsoleAppender;
import java.util.Locale;
import org.slf4j.LoggerFactory;
import org.slf4j.bridge.SLF4JBridgeHandler;
import org.slf4j.event.Level;

public class StructuredLogging {

  private static final String DEFAULT_LOGGING_LEVEL = System.getenv().getOrDefault("FLO_LOGGING_LEVEL", "INFO");

  private StructuredLogging() {
    throw new UnsupportedOperationException();
  }

  public static void configureStructuredLogging() {
    configureStructuredLogging(defaultLoggingLevel());
  }

  public static void configureStructuredLogging(Level level) {
    final Logger rootLogger = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);

    final LoggerContext context = rootLogger.getLoggerContext();
    context.reset();

    final StructuredLoggingEncoder encoder = new StructuredLoggingEncoder();
    encoder.start();

    final ConsoleAppender<ILoggingEvent> appender = new ConsoleAppender<ILoggingEvent>();
    appender.setTarget("System.err");
    appender.setName("stderr");
    appender.setEncoder(encoder);
    appender.setContext(context);
    appender.start();

    rootLogger.addAppender(appender);
    rootLogger.setLevel(fromLocationAwareLoggerInteger(level.toInt()));

    rootLogger.info("hello");

    SLF4JBridgeHandler.install();
  }

  private static Level defaultLoggingLevel() {
    return Level.valueOf(DEFAULT_LOGGING_LEVEL.toUpperCase(Locale.ROOT));
  }
}
