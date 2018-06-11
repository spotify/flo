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

import com.spotify.flo.EvalContext;
import com.spotify.flo.Fn;
import com.spotify.flo.TaskId;
import com.spotify.flo.Tracing;
import com.spotify.flo.freezer.PersistingContext;
import io.grpc.Context;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An {@link EvalContext} that runs tasks in a separate {@link ClassLoader}. All inputs and outputs of a task must be
 * serializable. This includes exceptions.
 */
public class IsolatingEvalContext extends ForwardingEvalContext {

  private static final Logger log = LoggerFactory.getLogger(ForwardingEvalContext.class);

  private static boolean FLO_DISABLE_ISOLATION = Boolean.parseBoolean(System.getenv("FLO_DISABLE_ISOLATION"));

  private static volatile TaskId currentTaskId;

  private IsolatingEvalContext(EvalContext delegate) {
    super(delegate);
  }

  public static EvalContext composeWith(EvalContext baseContext) {
    return new IsolatingEvalContext(baseContext);
  }

  /**
   * Returns the current {@link TaskId} when called from within an isolated task. E.g. by a logging implementation.
   */
  public static TaskId currentTaskId() {
    return currentTaskId;
  }

  @Override
  public <T> Value<T> value(Fn<T> value) {
    if (FLO_DISABLE_ISOLATION) {
      log.debug("Isolation disabled");
      return super.value(value);
    } else {
      return super.value(isolate(value));
    }
  }

  @SuppressWarnings("unchecked")
  private <T> Fn<T> isolate(Fn<T> value) {
    return () -> {

      final String[] classPath = System.getProperty("java.class.path").split(":");
      Path tempdir = null;

      try {
        try {
          tempdir = Files.createTempDirectory("flo-isolation");
        } catch (IOException e) {
          throw new RuntimeException(e);
        }

        // XXX: We marshal task result and error using serialization in and out of the task class loader in order to
        // avoid mixing classes from different class loaders. E.g., if a FoobarException leaks from the task class
        // class loader, it will not work as expected in a catch clause or instanceof statement in main class loader.

        // TODO: This will not work in an environment without a writable filesystem.

        final Path closureFile = tempdir.resolve("closure");
        final Path resultFile = tempdir.resolve("result");
        final Path errorFile = tempdir.resolve("error");

        log.debug("serializing closure");
        try {
          PersistingContext.serialize(value, closureFile);
        } catch (Exception e) {
          throw new RuntimeException("Failed to serialize closure", e);
        }

        final URL[] urls = Stream.of(classPath).map(this::toUrl).toArray(URL[]::new);
        final URLClassLoader classLoader = new URLClassLoader(urls, null);

        final Class<?> trampolineClass;
        try {
          trampolineClass = classLoader.loadClass(Trampoline.class.getName());
        } catch (ClassNotFoundException e) {
          throw new RuntimeException(e);
        }

        final Method runMethod;
        try {
          runMethod = trampolineClass.getDeclaredMethod("run", String.class, String.class, String.class, String.class);
        } catch (NoSuchMethodException e) {
          throw new RuntimeException(e);
        }
        runMethod.setAccessible(true);

        final String taskId = Tracing.TASK_ID.get().toString();
        try {
          runMethod.invoke(null, taskId, closureFile.toString(), resultFile.toString(), errorFile.toString());
        } catch (IllegalAccessException | InvocationTargetException e) {
          throw new RuntimeException(e);
        }

        if (Files.exists(errorFile)) {
          // Failed
          log.debug("task exited with error file");
          final Throwable error;
          try {
            error = PersistingContext.deserialize(errorFile);
          } catch (Exception e) {
            throw new RuntimeException("Failed to deserialize error", e);
          }
          if (error instanceof RuntimeException) {
            throw (RuntimeException) error;
          } else {
            throw new RuntimeException(error);
          }
        } else {
          // Success
          log.debug("task exited with result file");
          final T result;
          try {
            result = PersistingContext.deserialize(resultFile);
          } catch (Exception e) {
            throw new RuntimeException("Failed to deserialize result", e);
          }
          return result;
        }
      } finally {
        tryDeleteDir(tempdir);
      }
    };
  }

  private static void tryDeleteDir(Path path) {
    try {
      deleteDir(path);
    } catch (IOException e) {
      LOG.warn("Failed to delete directory: {}", path, e);
    }
  }

  private static void deleteDir(Path path) throws IOException {
    Files.walkFileTree(path, new SimpleFileVisitor<Path>() {
      @Override
      public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
        Files.delete(file);
        return FileVisitResult.CONTINUE;
      }

      @Override
      public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
        Files.delete(dir);
        return FileVisitResult.CONTINUE;
      }
    });
  }

  private URL toUrl(String path) {
    if (!path.endsWith(".jar") && !path.endsWith("/")) {
      path += "/";
    }
    try {
      return new URL("file:" + path);
    } catch (MalformedURLException e) {
      throw new RuntimeException(e);
    }
  }

  private static class Trampoline {

    @SuppressWarnings("unchecked")
    private static void run(String taskIdString, String closureFile, String resultFile, String errorFile)
        throws Exception {
      log.debug("deserializing closure");
      final Fn<?> fn = PersistingContext.deserialize(Paths.get(closureFile));

      final TaskId taskId = TaskId.parse(taskIdString);
      IsolatingEvalContext.currentTaskId = taskId;

      log.debug("executing closure");
      try {
        final Object result = Context.current()
            .withValue(Tracing.TASK_ID, taskId)
            .call(fn::get);
        PersistingContext.serialize(result, Paths.get(resultFile));
      } catch (Exception e) {
        PersistingContext.serialize(e, Paths.get(errorFile));
      }
    }
  }
}
