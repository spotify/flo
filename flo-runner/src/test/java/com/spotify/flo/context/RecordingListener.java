/*-
 * -\-\-
 * Flo Runner
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

package com.spotify.flo.context;

import com.google.auto.service.AutoService;
import com.spotify.flo.Serialization;
import com.spotify.flo.Task;
import com.spotify.flo.TaskId;
import com.spotify.flo.context.InstrumentedContext.Listener;
import com.spotify.flo.freezer.PersistingContext;
import com.typesafe.config.Config;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * Records {@link InstrumentedContext.Listener} invocations to disk for later replay.
 *
 * For use by {@link FloRunnerTest} when testing the {@link InstrumentedContext} mechanism.
 */
@AutoService(FloListenerFactory.class)
public class RecordingListener implements FloListenerFactory {

  @Override
  public Listener createListener(Config config) {
    return new Recorder(FloRunnerTest.listenerOutputDir);
  }

  private static class Recorder implements InstrumentedContext.Listener {

    private Recorder(String dir) {
      this.dir = Objects.requireNonNull(dir);
    }

    private final String dir;

    @Override
    public void task(Task<?> task) {
      record(listener -> listener.task(task));
    }

    @Override
    public void status(TaskId taskId, Phase phase) {
      record(listener -> listener.status(taskId, phase));
    }

    @Override
    public void meta(TaskId task, Map<String, String> data) {
      record(listener -> listener.meta(task, data));
    }

    /**
     * Persist the listener invocation
     */
    private void record(SerializableConsumer<Listener> event) {
      if (dir == null) {
        return;
      }
      final Path path = Paths.get(dir, eventFilename());
      try {
        Serialization.serialize(event, path);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    /**
     * Best-effort generate a unique chronological file name
     */
    private static String eventFilename() {
      return Long.toHexString(System.currentTimeMillis()) + '-' +
          Long.toHexString(System.nanoTime()) + '-' +
          Long.toHexString(ThreadLocalRandom.current().nextLong());
    }
  }

  /**
   * Replay all the recorded listener invocations onto the provided {@link InstrumentedContext.Listener}.
   */
  static void replay(Listener listener) throws IOException, ClassNotFoundException {
    final List<Path> files = Files.list(Paths.get(FloRunnerTest.listenerOutputDir)).sorted()
        .collect(Collectors.toList());
    for (Path file : files) {
      final Consumer<Listener> event = Serialization.deserialize(file);
      event.accept(listener);
    }
  }
}
