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

package com.spotify.flo.execute.scio;

import static com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import static java.util.stream.Collectors.toList;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.spotify.flo.Task;
import com.spotify.flo.TaskId;
import com.spotify.flo.contrib.scio.ScioJobSpec;
import com.spotify.flo.deploy.models.DataflowJob;
import com.spotify.flo.deploy.models.DataflowJobBuilder;
import com.spotify.flo.deploy.models.ExecutionManifest;
import com.spotify.flo.deploy.models.Workflow;
import com.spotify.flo.deploy.models.WorkflowManifest;
import com.spotify.flo.freezer.EvaluatingContext;
import com.spotify.flo.freezer.PersistingContext;
import com.spotify.scio.ScioContext;
import com.spotify.scio.ScioResult;
import io.norberg.automatter.jackson.AutoMatterModule;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import org.apache.beam.runners.dataflow.DataflowPipelineJob;
import org.apache.beam.sdk.options.PipelineOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExecuteScio {

  private static final Logger log = LoggerFactory.getLogger(ExecuteScio.class);

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
      .registerModule(new AutoMatterModule())
      .disable(FAIL_ON_UNKNOWN_PROPERTIES);

  private static final ExecutorService EXECUTOR = new ForkJoinPool(32);

  public static void main(String... args) throws IOException, InterruptedException, ExecutionException, TimeoutException {
    log.info("flo-task-execute-scio: manifest={}, taskId={}, command={}, dataflowJobReference={}",
        args[0], args[1], args[2], args[3]);

    final Path executionManifestLocation = Paths.get(URI.create(args[0]));
    final TaskId taskId = TaskId.parse(args[1]);
    final String command = args[2];
    final Path dataflowJobReferenceLocation = Paths.get(URI.create(args[3]));

    final Path tempdir = Files.createTempDirectory("flo-task-execute-scio");

    final ExecutionManifest executionManifest = OBJECT_MAPPER.readValue(Files.readAllBytes(
        executionManifestLocation), ExecutionManifest.class);

    final WorkflowManifest workflowManifest = OBJECT_MAPPER.readValue(Files.readAllBytes(
        Paths.get(executionManifest.workflowManifest())), WorkflowManifest.class);

    final Path workflowStagingLocation = Paths.get(workflowManifest.stagingLocation());
    final Path executionStagingLocation = Paths.get(executionManifest.stagingLocation());

    final Path workflowFileLocation = executionStagingLocation
        .resolve(executionManifest.workflowFile());

    final Workflow workflow = OBJECT_MAPPER.readValue(Files.readAllBytes(workflowFileLocation), Workflow.class);

    final Workflow.Task task = workflow.tasks().stream()
        .filter(t -> taskId.toString().equals(t.id()))
        .findFirst()
        .get();

    // Download upstream results
    log.info("Downloading upstream results: {}", task.upstreams());
    final List<Path> localUpstreamResultFiles = task.upstreams().stream()
        .map(id -> PersistingContext.cleanForFilename(TaskId.parse(id)) + "_out")
        .map(executionStagingLocation::resolve)
        .map(resultFile -> downloadAsync(resultFile, tempdir))
        .collect(Collectors.toList()).stream()
        .map(CompletableFuture::join)
        .collect(Collectors.toList());

    // Download jars
    log.info("Downloading jars: {}", workflowManifest.files());
    final List<Path> files = workflowManifest.files().stream()
        .map(f -> CompletableFuture.supplyAsync(() ->
            download(workflowStagingLocation.resolve(f), tempdir), EXECUTOR))
        .collect(toList()).stream()
        .map(CompletableFuture::join)
        .collect(Collectors.toList());
    final URL[] classpath = files.stream().map(ExecuteScio::toURL).toArray(URL[]::new);

    // TODO: Full isolation?
    final URLClassLoader workflowClassLoader = new URLClassLoader(classpath, ClassLoader.getSystemClassLoader());

    // TODO: Is setting the CCL the way to go here?
    Thread.currentThread().setContextClassLoader(workflowClassLoader);

    final String taskFile = executionManifest.taskFiles().get(taskId);
    final Path taskFileLocation = executionStagingLocation.resolve(taskFile);

    final Path localTaskFile = tempdir.resolve(taskFile);
    Files.copy(taskFileLocation, localTaskFile);

    final Task<?> floTask = PersistingContext.deserialize(localTaskFile);

    // Extract scio job spec
    log.info("Executing task: {}", taskId);
    final CompletableFuture<ScioJobSpec> specFuture = new CompletableFuture<>();
    evaluationContext(tempdir, taskId, specFuture)
        .evaluateTaskFrom(localTaskFile)
        .toFuture().get(30, TimeUnit.SECONDS);

    switch (command) {
      case "start":
        final ScioJobSpec jobSpec = specFuture.get(30, TimeUnit.SECONDS);
        final PipelineOptions options = (PipelineOptions) jobSpec.options().apply();

        final ScioContext scioContext = ScioContext.apply(options);

        //noinspection unchecked
        jobSpec.pipeline().apply(scioContext);

        final ScioResult scioResult = scioContext.close();
        final DataflowPipelineJob dataflowJob = (DataflowPipelineJob) scioResult.internal();

        final DataflowJob job = new DataflowJobBuilder()
            .jobId(dataflowJob.getJobId())
            .region(dataflowJob.getRegion())
            .projectId(dataflowJob.getProjectId())
            .options(OBJECT_MAPPER.convertValue(options, new TypeReference<Map<String, Object>>() { }))
            .build();

        // Upload dataflow job file
        final String dataflowJobFileName = PersistingContext.cleanForFilename(floTask.id()) + "_df_job";
        final Path remotedataflowJobFileLocation = executionStagingLocation.resolve(dataflowJobFileName);
        log.info("Writing data flow job file: {}", remotedataflowJobFileLocation.toUri());
        Files.write(remotedataflowJobFileLocation, OBJECT_MAPPER.writeValueAsBytes(job));

      case "finish":
        throw new UnsupportedOperationException("TODO");

      default:
        throw new AssertionError();
    }
  }

  private static CompletableFuture<Path> downloadAsync(Path src, Path dst) {
    return CompletableFuture.supplyAsync(() -> download(src, dst), EXECUTOR);
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

  private static URL toURL(Path path) {
    try {
      return path.toUri().toURL();
    } catch (MalformedURLException e) {
      throw new RuntimeException(e);
    }
  }

  private static EvaluatingContext evaluationContext(Path persistedTasksDir, TaskId taskId,
      CompletableFuture<ScioJobSpec> specFuture) {
    return new EvaluatingContext(persistedTasksDir, new ScioJobSpecExtractingContext(taskId, specFuture));
  }

}
