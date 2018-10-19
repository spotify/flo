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

import com.spotify.flo.Fn;
import com.spotify.flo.Serialization;
import com.spotify.flo.freezer.PersistingContext;
import java.io.BufferedReader;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.lang.management.ManagementFactory;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An executor that executes a {@link Fn} in a sub-process JVM.
 * <p>
 * The function, its result and any thrown exception must be serializable as
 * serialization is used to transport these between the processes.
 */
class ForkingExecutor implements Closeable {

  private static final Logger log = LoggerFactory.getLogger(ForkingExecutor.class);

  private final List<Execution> executions = new ArrayList<>();

  private Map<String, String> environment = Collections.emptyMap();
  private List<String> javaArgs = Collections.emptyList();

  ForkingExecutor environment(Map<String, String> environment) {
    this.environment = new HashMap<>(environment);
    return this;
  }

  ForkingExecutor javaArgs(String... javaArgs) {
    return javaArgs(Arrays.asList(javaArgs));
  }

  ForkingExecutor javaArgs(List<String> javaArgs) {
    this.javaArgs = new ArrayList<>(javaArgs);
    return this;
  }

  /**
   * Execute a function in a sub-process.
   *
   * @param f The function to execute.
   * @return The return value of the function. Any exception thrown by the
   *         function the will be propagated and re-thrown.
   * @throws IOException if
   */
  <T> T execute(Fn<T> f) throws IOException {
    try (final Execution<T> execution = new Execution<>(f)) {
      executions.add(execution);
      execution.start();
      return execution.waitFor();
    }
  }

  @Override
  public void close() {
    executions.forEach(Execution::close);
  }

  private class Execution<T> implements Closeable {

    private final ExecutorService executor = Executors.newCachedThreadPool();

    private final Path tempdir = Files.createTempDirectory("flo-fork");

    private final Path workdir = Files.createDirectory(tempdir.resolve("workdir"));

    private final Path closureFile = tempdir.resolve("closure");
    private final Path resultFile = tempdir.resolve("result");
    private final Path errorFile = tempdir.resolve("error");

    private final String home = System.getProperty("java.home");
    private final String classPath = System.getProperty("java.class.path");
    private final Path java = Paths.get(home, "bin", "java").toAbsolutePath().normalize();

    private final Fn<T> f;

    private Process process;

    Execution(Fn<T> f) throws IOException {
      this.f = Objects.requireNonNull(f);
    }

    void start() {
      if (process != null) {
        throw new IllegalStateException();
      }
      log.debug("serializing closure");
      try {
        Serialization.serialize(f, closureFile);
      } catch (Exception e) {
        throw new RuntimeException("Failed to serialize closure", e);
      }

      final ProcessBuilder processBuilder = new ProcessBuilder(java.toString(), "-cp", classPath)
          .directory(workdir.toFile());

      // Propagate -Xmx and -D.
      // Note: This is suboptimal because if the user has configured a max heap size we will effectively use that
      // times the concurrent nummber of executing task processes in addition to the heap of the parent process.
      // However, propagating a lower limit might make the task fail if the user has supplied a heap size that is
      // tailored to the memory requirements of the task.
      ManagementFactory.getRuntimeMXBean().getInputArguments().stream()
          .filter(s -> s.startsWith("-Xmx") || s.startsWith("-D"))
          .forEach(processBuilder.command()::add);

      // Custom jvm args
      javaArgs.forEach(processBuilder.command()::add);

      // Trampoline arguments
      processBuilder.command().add(Trampoline.class.getName());
      processBuilder.command().add(closureFile.toString());
      processBuilder.command().add(resultFile.toString());
      processBuilder.command().add(errorFile.toString());

      processBuilder.environment().putAll(environment);

      log.debug("Starting subprocess: environment={}, command={}, directory={}",
          processBuilder.environment(), processBuilder.command(), processBuilder.directory());
      try {
        process = processBuilder.start();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }

      // Copy std{err,out} line by line to avoid interleaving and corrupting line contents.
      executor.submit(() -> copyLines(process.getInputStream(), System.out));
      executor.submit(() -> copyLines(process.getErrorStream(), System.err));
    }

    T waitFor() {
      if (process == null) {
        throw new IllegalStateException();
      }
      log.debug("Waiting for subprocess exit");
      final int exitValue;
      try {
        exitValue = process.waitFor();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(e);
      } finally {
        process.destroyForcibly();
      }

      log.debug("Subprocess exited: " + exitValue);
      if (exitValue != 0) {
        throw new RuntimeException("Subprocess failed: " + process.exitValue());
      }

      if (Files.exists(errorFile)) {
        // Failed
        log.debug("Subprocess exited with error file");
        final Throwable error;
        try {
          error = Serialization.deserialize(errorFile);
        } catch (Exception e) {
          throw new RuntimeException("Failed to deserialize error", e);
        }
        if (error instanceof Error) {
          throw (Error) error;
        } else if (error instanceof RuntimeException) {
          throw (RuntimeException) error;
        } else {
          throw new RuntimeException(error);
        }
      } else {
        // Success
        log.debug("Subprocess exited with result file");
        final T result;
        try {
          result = Serialization.deserialize(resultFile);
        } catch (Exception e) {
          throw new RuntimeException("Failed to deserialize result", e);
        }
        return result;
      }
    }

    @Override
    public void close() {
      if (process != null) {
        process.destroyForcibly();
        process = null;
      }
      executor.shutdown();
      tryDeleteDir(tempdir);
    }
  }

  private static void tryDeleteDir(Path path) {
    try {
      deleteDir(path);
    } catch (IOException e) {
      log.warn("Failed to delete directory: {}", path, e);
    }
  }

  private static void deleteDir(Path path) throws IOException {
    try {
      Files.walkFileTree(path, new SimpleFileVisitor<Path>() {
        @Override
        public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
          try {
            Files.delete(file);
          } catch (NoSuchFileException ignore) {
          }
          return FileVisitResult.CONTINUE;
        }

        @Override
        public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
          try {
            Files.delete(dir);
          } catch (NoSuchFileException ignore) {
          }
          return FileVisitResult.CONTINUE;
        }
      });
    } catch (NoSuchFileException ignore) {
    }
  }

  private static void copyLines(InputStream in, PrintStream out) {
    final BufferedReader reader = new BufferedReader(new InputStreamReader(in));
    try {
      String line;
      while ((line = reader.readLine()) != null) {
        out.println(line);
      }
    } catch (IOException e) {
      log.error("Caught exception during stream copy", e);
    }
  }

  private static class Trampoline {

    private static class Watchdog extends Thread {

      Watchdog() {
        setDaemon(true);
      }

      @Override
      public void run() {
        // Wait for parent to exit.
        try {
          while (true) {
            int c = System.in.read();
            if (c == -1) {
              break;
            }
          }
        } catch (IOException e) {
          log.error("watchdog failed", e);
        }
        log.debug("child process exiting");
        // Exit with non-zero status code to skip shutdown hooks
        System.exit(-1);
      }
    }

    public static void main(String... args) {
      log.debug("child process started: args={}", Arrays.asList(args));
      final Trampoline.Watchdog watchdog = new Trampoline.Watchdog();
      watchdog.start();

      if (args.length != 3) {
        log.error("args.length != 3");
        System.exit(3);
        return;
      }
      final Path closureFile;
      final Path resultFile;
      final Path errorFile;
      try {
        closureFile = Paths.get(args[0]);
        resultFile = Paths.get(args[1]);
        errorFile = Paths.get(args[2]);
      } catch (InvalidPathException e) {
        log.error("Failed to get file path", e);
        System.exit(4);
        return;
      }

      run(closureFile, resultFile, errorFile);
    }

    private static void run(Path closureFile, Path resultFile, Path errorFile) {
      log.debug("deserializing closure: {}", closureFile);
      final Fn<?> fn;
      try {
        fn = Serialization.deserialize(closureFile);
      } catch (Exception e) {
        log.error("Failed to deserialize closure: {}", closureFile, e);
        System.exit(5);
        return;
      }

      log.debug("executing closure");
      Object result = null;
      Throwable error = null;
      try {
        result = fn.get();
      } catch (Throwable e) {
        error = e;
      }

      if (error != null) {
        log.debug("serializing error", error);
        try {
          Serialization.serialize(error, errorFile);
        } catch (Exception e) {
          log.error("failed to serialize error", e);
          System.exit(6);
          return;
        }
      } else {
        log.debug("serializing result: {}", result);
        try {
          Serialization.serialize(result, resultFile);
        } catch (Exception e) {
          log.error("failed to serialize result", e);
          System.exit(7);
          return;
        }
      }

      System.err.flush();
      System.exit(0);
    }
  }
}
