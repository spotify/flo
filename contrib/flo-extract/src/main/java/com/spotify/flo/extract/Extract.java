/*-
 * -\-\-
 * Flo Extract
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

package com.spotify.flo.extract;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import static java.util.stream.Collectors.toList;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.Tuple;
import com.spotify.flo.EvalContext;
import com.spotify.flo.OperationExtractingContext;
import com.spotify.flo.Task;
import com.spotify.flo.TaskId;
import com.spotify.flo.TaskInfo;
import com.spotify.flo.TaskOperator;
import com.spotify.flo.context.MemoizingContext;
import com.spotify.flo.context.PrintUtilsBridge;
import com.spotify.flo.freezer.PersistingContext;
import com.spotify.flo.util.Date;
import com.spotify.flo.util.DateHour;
import io.norberg.automatter.jackson.AutoMatterModule;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import java.net.URLEncoder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Extract {

  private static final Logger log = LoggerFactory.getLogger(Extract.class);

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
      .registerModule(new AutoMatterModule());
  private static final ExecutorService EXECUTOR = new ForkJoinPool(32);

  public static void main(String[] args) throws IOException, ReflectiveOperationException {
    final URI manifestUri = URI.create(args[0]);
    final String param = args[1];

    extract(manifestUri, param);
  }

  static void extract(URI workflowManifestUri, String param) throws IOException, ReflectiveOperationException {
    log.info("extract: manifestUri={}, param={}", workflowManifestUri, param);

    final byte[] manifestBytes = Files.readAllBytes(Paths.get(workflowManifestUri));
    final WorkflowManifest workflowManifest = OBJECT_MAPPER.readValue(manifestBytes, WorkflowManifest.class);

    final Path workflowStagingLocation = Paths.get(workflowManifest.stagingLocation());
    final Path executionStagingLocation = workflowStagingLocation.resolve(
        "execution-" + URLEncoder.encode(param, "UTF-8") + "-" +
            Long.toHexString(ThreadLocalRandom.current().nextLong()));

    log.debug("workflowStagingLocation: {}", workflowStagingLocation.toUri());
    log.debug("executionStagingLocation: {}", executionStagingLocation.toUri());

    final Path tempdir = Files.createTempDirectory(null);
    final Path executionDir = Files.createTempDirectory(null);

    log.debug("tempdir: {}", tempdir);
    log.debug("executionDir: {}", executionDir);

    workflowManifest.files().stream()
        .map(f -> CompletableFuture.supplyAsync(() ->
            download(workflowStagingLocation.resolve(f), tempdir), EXECUTOR))
        .collect(toList())
        .forEach(CompletableFuture::join);

    final String parameterTypeName = workflowManifest.entryPoint().parameterType();
    final Class<?> parameterType;
    final Object arg;
    if (parameterTypeName.equals(Date.class.getName())) {
      parameterType = Date.class;
      arg = Date.parse(param);
    } else if (parameterTypeName.equals(DateHour.class.getName())) {
      parameterType = DateHour.class;
      arg = DateHour.parse(param);
    } else if (parameterTypeName.equals(String.class.getName())) {
      parameterType = String.class;
      arg = param;
    } else {
      throw new IllegalArgumentException("Unsupported parameter type: " + parameterTypeName);
    }

    log.info("Loading workflow entrypoint: {}#{}",
        workflowManifest.entryPoint().klass(), workflowManifest.entryPoint().method());
    final URLClassLoader workflowClassLoader = new URLClassLoader(new URL[]{tempdir.toUri().toURL()},
        ClassLoader.getSystemClassLoader());
    final Class<?> klass = workflowClassLoader.loadClass(workflowManifest.entryPoint().klass());
    final Method entrypoint = klass.getDeclaredMethod(workflowManifest.entryPoint().method(), parameterType);
    entrypoint.setAccessible(true);

    log.info("Invoking workflow entrypoint: {}#{}",
        workflowManifest.entryPoint().klass(), workflowManifest.entryPoint().method());
    final Task<?> root = (Task<?>) entrypoint.invoke(null, arg);

    logWorkflow(root);

    stageExecution(executionDir, root, executionStagingLocation, workflowManifestUri);
  }

  private static void logWorkflow(Task<?> root) {
    log.info("Got workflow tasks:");
    final TaskInfo taskInfo = TaskInfo.ofTask(root);
    PrintUtilsBridge.tree(taskInfo).forEach(log::info);

  }

  private static Path download(Path srcFile, Path dstDir) {
    final Path destinationFile = dstDir.resolve(srcFile.getFileName().toString());
    log.debug("Downloading {} to {}", srcFile.toUri(), destinationFile);
    try {
      Files.copy(srcFile, destinationFile, REPLACE_EXISTING);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return destinationFile;
  }

  private static void stageExecution(Path dir, Task<?> root, Path executionStagingLocation,
      URI workflowManifestUri) throws IOException {
    log.info("Staging execution: {}", executionStagingLocation.toUri());

    final PersistingContext persistingContext = new PersistingContext(dir, EvalContext.sync());

    final Workflow workflow = workflow(root);
    final String workflowJson = OBJECT_MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(workflow);
    // TODO: retries
    Files.write(executionStagingLocation.resolve("workflow.json"), workflowJson.getBytes(UTF_8));

    try {
      MemoizingContext.composeWith(persistingContext)
          .evaluate(root).toFuture().exceptionally(t -> null).get();
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException(e);
    }

    final ExecutionManifestBuilder manifestBuilder = new ExecutionManifestBuilder();
    manifestBuilder.workflowFile("workflow.json");

    final Map<TaskId, String> stagedTaskFiles = persistingContext.getFiles().entrySet().stream()
        .map(e -> CompletableFuture.supplyAsync(() -> {
          final TaskId id = e.getKey();
          final Path source = e.getValue();
          final String name = PersistingContext.cleanForFilename(id);
          final Path target = executionStagingLocation.resolve(name);
          log.debug("Uploading {} to {}", source, target.toUri());
          try {
            // TODO: retries
            Files.copy(source, target, REPLACE_EXISTING);
          } catch (IOException ex) {
            throw new RuntimeException(ex);
          }
          return Tuple.of(id, name);
        }, EXECUTOR))
        .collect(toList()).stream()
        .map(CompletableFuture::join)
        .collect(Collectors.toMap(Tuple::x, Tuple::y));

    stagedTaskFiles.forEach((id, name) -> {
      manifestBuilder.putTaskFile(id.toString(), name);
    });

    manifestBuilder.workflowManifest(workflowManifestUri);
    manifestBuilder.stagingLocation(executionStagingLocation.toUri());

    final ExecutionManifest manifest = manifestBuilder.build();

    // TODO: retries
    final Path manifestLocation = executionStagingLocation.resolve("manifest.json");
    log.debug("Writing manifest to {}", manifestLocation.toUri());
    Files.write(manifestLocation, OBJECT_MAPPER.writeValueAsBytes(manifest));
  }

  private static Workflow workflow(Task<?> root) {
    final List<Task<?>> tasks = new ArrayList<>();
    final Set<TaskId> visited = new HashSet<>();
    enumerateTasks(root, tasks, visited);

    final WorkflowBuilder builder = new WorkflowBuilder();
    for (Task<?> task : tasks) {
      final Optional<? extends TaskOperator<?, ?, ?>> operator = OperationExtractingContext.operator(task);
      final TaskBuilder taskBuilder = new TaskBuilder()
          .operator(operator.map(o -> o.getClass().getName()).orElse("<generic>"))
          .id(task.id().toString());
      for (Task<?> upstream : task.inputs()) {
        taskBuilder.addUpstream(upstream.id().toString());
      }
      builder.addTask(taskBuilder.build());
    }

    return builder.build();
  }

  private static void enumerateTasks(Task<?> task, List<Task<?>> tasks, Set<TaskId> visited) {
    if (!visited.add(task.id())) {
      return;
    }
    for (Task<?> input : task.inputs()) {
      enumerateTasks(input, tasks, visited);
    }
    tasks.add(task);
  }
}
