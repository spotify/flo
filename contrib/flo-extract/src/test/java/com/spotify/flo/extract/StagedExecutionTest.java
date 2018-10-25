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

package com.spotify.flo.extract;

import static java.util.stream.Collectors.toList;

import com.fasterxml.jackson.core.util.DefaultPrettyPrinter;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.spotify.flo.EvalContext;
import com.spotify.flo.Fn;
import com.spotify.flo.OperationExtractingContext;
import com.spotify.flo.Task;
import com.spotify.flo.TaskId;
import com.spotify.flo.TaskOperator;
import com.spotify.flo.TaskOutput;
import com.spotify.flo.extract.StagingUtil.StagedPackage;
import com.spotify.flo.freezer.EvaluatingContext;
import com.spotify.flo.util.Date;
import io.norberg.automatter.jackson.AutoMatterModule;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URLEncoder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Consumer;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A POC for staged workflow execution.
 */
public class StagedExecutionTest {

  private static final Logger log = LoggerFactory.getLogger(StagedExecutionTest.class);

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
      .setDefaultPrettyPrinter(new DefaultPrettyPrinter())
      .registerModule(new AutoMatterModule());

  private final ExecutorService executor = Executors.newWorkStealingPool(32);

  private final URI stagingLocation = URI.create("gs://dano-test/staging/");
  private final URI workflowsStagingLocation = stagingLocation.resolve("workflows");
  private final URI executionsStagingLocation = stagingLocation.resolve("executions");

  private final String parameter = "2018-10-23";
  private final String executionId = "execution-" + urlencode(parameter) + "-" + UUID.randomUUID();

  private final URI workflowStagingLocation = workflowsStagingLocation.resolve("test");
  private final URI executionStagingLocation = executionsStagingLocation.resolve(executionId);

  @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @After
  public void tearDown() {
    executor.shutdownNow();
  }

  @Test
  public void testStageAndExtract() throws IOException, ReflectiveOperationException {
    final URI manifestUri = stageWorkflow(
        "com.spotify.flo.extract.StagedExecutionTest", "workflow",
        Date.class, workflowStagingLocation);
    Extract.extract(manifestUri, parameter, executionStagingLocation);
  }

  @Test
  public void testStage() {
    stageWorkflow("com.spotify.flo.extract.StagedExecutionTest", "workflow", Date.class, workflowStagingLocation);
  }

  @Test
  public void testExtract() throws IOException, ReflectiveOperationException {
    final URI manifestUri = URI.create("gs://dano-test/staging/workflow-manifest-19ed68f0f1ae60e3.json");
    Extract.extract(manifestUri, parameter, executionStagingLocation);
  }

  @SuppressWarnings("unused")
  static Task<?> workflow(Date date) {

    final Fn<Task<String>> fooTask = () -> Task.named("foo")
        .ofType(String.class)
        .process(() -> {
          log.info("process fn: foo: " + date);
          return "foo(" + date + ")";
        });

    final Task<String> barTask = Task.named("bar")
        .ofType(String.class)
        .input(fooTask)
        .process(foo -> {
          log.info("process fn: bar");
          return foo + " bar(" + date + ")";
        });

    return barTask;
  }

  private static URI stageWorkflow(String klass, String method, Class<?> parameterType,
      URI workflowStagingLocation) {
    final ClasspathInspector classpathInspector = ClasspathInspector.forLoader(
        StagedExecutionTest.class.getClassLoader());
    final List<Path> workflowFiles = classpathInspector.classpathJars();

    final List<String> fileString = workflowFiles.stream()
        .map(Path::toAbsolutePath)
        .map(Path::toString)
        .collect(toList());

    final List<StagedPackage> stagedPackages = StagingUtil.stageClasspathElements(
        fileString, workflowStagingLocation.toString());

    final WorkflowManifestBuilder manifestBuilder = new WorkflowManifestBuilder();
    manifestBuilder.entryPoint(new EntryPointBuilder()
        .klass(klass)
        .method(method)
        .parameterType(parameterType.getName())
        .build());
    manifestBuilder.stagingLocation(workflowStagingLocation);
    stagedPackages.stream().map(StagedPackage::name).forEach(manifestBuilder::addFile);
    final WorkflowManifest manifest = manifestBuilder.build();

    final String manifestName = "workflow-manifest-" + Long.toHexString(ThreadLocalRandom.current().nextLong()) +".json";

    final Path manifestLocation = Paths.get(workflowStagingLocation).resolve(manifestName);

    try {
      Files.write(manifestLocation,
          OBJECT_MAPPER.writerWithDefaultPrettyPrinter().writeValueAsBytes(manifest));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    log.info("Staged workflow to: {}", manifestLocation.toUri());

    return manifestLocation.toUri();
  }

  private static List<String> toStrings(List<Path> paths) {
    return paths.stream()
        .map(Path::toAbsolutePath)
        .map(Path::toString)
        .collect(toList());
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

  private static void tasks(Task<?> task, Consumer<Task<?>> f) {
    f.accept(task);
    task.inputs().forEach(upstream -> tasks(upstream, f));
  }

  private static Set<Task<?>> tasks(Task<?> task) {
    final Set<Task<?>> tasks = new HashSet<>();
    tasks(task, tasks::add);
    return tasks;
  }

  private static String urlencode(String s) {
    try {
      return URLEncoder.encode(s, "UTF-8");
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException(e);
    }
  }

  private static class DoneOutput<T> extends TaskOutput<String, T> {

    private final T value;

    DoneOutput(T value) {
      this.value = value;
    }

    @Override
    public String provide(EvalContext evalContext) {
      return "";
    }

    @Override
    public Optional<T> lookup(Task<T> task) {
      return Optional.of(value);
    }
  }
}
