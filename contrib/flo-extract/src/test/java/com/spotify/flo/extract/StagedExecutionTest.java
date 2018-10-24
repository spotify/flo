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
import com.spotify.flo.freezer.PersistingContext;
import com.spotify.flo.util.Date;
import io.norberg.automatter.jackson.AutoMatterModule;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Consumer;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A POC for staged workflow execution.
 */
public class StagedExecutionTest {

  private static URI stagingLocation = URI.create("gs://dano-test/staging/");
  private static final ExecutorService executor = Executors.newWorkStealingPool(32);

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
      .setDefaultPrettyPrinter(new DefaultPrettyPrinter())
      .registerModule(new AutoMatterModule());

  private static final Logger log = LoggerFactory.getLogger(StagedExecutionTest.class);

  @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void testStageAndExtract() throws IOException, ReflectiveOperationException {
    final URI manifestUri = stageWorkflow("com.spotify.flo.extract.StagedExecutionTest", "workflow", Date.class);
    Extract.extract(manifestUri, "2018-10-23");
  }

  @Test
  public void testStage() {
    stageWorkflow("com.spotify.flo.extract.StagedExecutionTest", "workflow", Date.class);
  }

  @Test
  public void testExtract() throws IOException, ReflectiveOperationException {
    final URI manifestUri = URI.create("gs://dano-test/staging/workflow-manifest-910c209b2eafc2bb.json");
    Extract.extract(manifestUri, "2018-10-23");
  }

  public static EvaluatingContext evaluationContext(String persistedTasksDir) {
    return new EvaluatingContext(Paths.get(persistedTasksDir), EvalContext.sync());
  }

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

  private static URI stageWorkflow(String klass, String method, Class<?> parameterType) {
    final ClasspathInspector classpathInspector = ClasspathInspector.forLoader(
        StagedExecutionTest.class.getClassLoader());
    final List<Path> workflowFiles = classpathInspector.classpathJars();

    final List<String> fileString = workflowFiles.stream()
        .map(Path::toAbsolutePath)
        .map(Path::toString)
        .collect(toList());

    final List<StagedPackage> stagedPackages = StagingUtil.stageClasspathElements(
        fileString, stagingLocation.toString());

    final WorkflowManifestBuilder manifestBuilder = new WorkflowManifestBuilder();
    manifestBuilder.entryPoint(new EntryPointBuilder()
        .klass(klass)
        .method(method)
        .parameterType(parameterType.getName())
        .build());
    manifestBuilder.stagingLocation(stagingLocation);
    stagedPackages.stream().map(StagedPackage::name).forEach(manifestBuilder::addFile);
    final WorkflowManifest manifest = manifestBuilder.build();

    final String manifestName = "workflow-manifest-" + Long.toHexString(ThreadLocalRandom.current().nextLong()) +".json";

    final Path manifestLocation = Paths.get(stagingLocation).resolve(manifestName);

    try {
      Files.write(manifestLocation,
          OBJECT_MAPPER.writerWithDefaultPrettyPrinter().writeValueAsBytes(manifest));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    System.err.println("Staged workflow to: " + manifestLocation.toUri());

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
          .id(task.id().toString())
          .payloadBase64(Base64.getEncoder().encodeToString(PersistingContext.serialize(task)));
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
