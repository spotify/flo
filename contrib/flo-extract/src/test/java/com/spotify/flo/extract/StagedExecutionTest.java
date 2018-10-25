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

import static com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES;
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
import com.spotify.flo.deploy.models.EntryPointBuilder;
import com.spotify.flo.deploy.models.TaskBuilder;
import com.spotify.flo.deploy.models.Workflow;
import com.spotify.flo.deploy.models.WorkflowBuilder;
import com.spotify.flo.deploy.models.WorkflowManifest;
import com.spotify.flo.deploy.models.WorkflowManifestBuilder;
import com.spotify.flo.extract.StagingUtil.StagedPackage;
import com.spotify.flo.hades.HadesTasks;
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
      .registerModule(new AutoMatterModule())
      .disable(FAIL_ON_UNKNOWN_PROPERTIES);

  private final ExecutorService executor = Executors.newWorkStealingPool(32);

  private final Path stagingLocation = Paths.get(URI.create("gs://dano-test/staging/"));
  private final Path workflowsStagingLocation = stagingLocation.resolve("workflows");
  private final Path executionsStagingLocation = stagingLocation.resolve("executions");

  private final String parameter = "2018-10-23";
  private final String executionId = "execution-" + urlencode(parameter) + "-" + UUID.randomUUID();

  private final Path workflowStagingLocation = workflowsStagingLocation.resolve("test/");
  private final Path executionStagingLocation = executionsStagingLocation.resolve(executionId + "/");

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
    final URI manifestUri = URI.create("gs://dano-test/staging/workflows/test/workflow-manifest-92e16212d7d06909.json");
    Extract.extract(manifestUri, parameter, executionStagingLocation);
  }

  @SuppressWarnings("unused")
  static Task<?> workflow(Date date) {
    return barTask(date);
  }

  private static Task<String> barTask(Date date) {
    return Task.named("bar")
        .ofType(String.class)
        .input(() -> fooTask(date))
        .input(() -> bazTask(date))
        .input(() -> HadesTasks.lookup("ScioStreamCountFlo", date))
        .process((foo, baz, streamCount) -> {
          final String result = foo + " bar(" + date + ") " + baz + " " + streamCount;
          log.info("process fn: bar: {}", result);
          return result;
        });
  }

  private static Task<String> fooTask(Date date) {
    return Task.named("foo")
        .ofType(String.class)
        .process(() -> {
          log.info("process fn: foo: " + date);
          return "foo(" + date + ")";
        });
  }

  private static Task<String> bazTask(Date date) {
    return Task.named("baz")
        .ofType(String.class)
        .process(() -> {
          log.info("process fn: baz: " + date);
          return "baz(" + date + ")";
        });
  }

  private static URI stageWorkflow(String klass, String method, Class<?> parameterType,
      Path workflowStagingLocation) {
    final ClasspathInspector classpathInspector = ClasspathInspector.forLoader(
        StagedExecutionTest.class.getClassLoader());
    final List<Path> workflowFiles = classpathInspector.classpathJars();

    final List<String> fileString = workflowFiles.stream()
        .map(Path::toAbsolutePath)
        .map(Path::toString)
        .collect(toList());

    final List<StagedPackage> stagedPackages = StagingUtil.stageClasspathElements(
        fileString, workflowStagingLocation.toUri().toString());

    final WorkflowManifestBuilder manifestBuilder = new WorkflowManifestBuilder();
    manifestBuilder.entryPoint(new EntryPointBuilder()
        .klass(klass)
        .method(method)
        .parameterType(parameterType.getName())
        .build());
    manifestBuilder.stagingLocation(workflowStagingLocation.toUri());
    stagedPackages.stream().map(StagedPackage::name).forEach(manifestBuilder::addFile);
    final WorkflowManifest manifest = manifestBuilder.build();

    final String manifestName = "workflow-manifest-" + Long.toHexString(ThreadLocalRandom.current().nextLong()) +".json";

    final Path manifestLocation = workflowStagingLocation.resolve(manifestName);

    try {
      Files.write(manifestLocation,
          OBJECT_MAPPER.writerWithDefaultPrettyPrinter().writeValueAsBytes(manifest));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    log.info("Staged workflow to: {}", manifestLocation.toUri());

    return manifestLocation.toUri();
  }

  private static String urlencode(String s) {
    try {
      return URLEncoder.encode(s, "UTF-8");
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException(e);
    }
  }
}
