/*-
 * -\-\-
 * flo-freezer
 * --
 * Copyright (C) 2016 - 2017 Spotify AB
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

package com.spotify.flo.freezer;

import static com.spotify.flo.TaskContext.inmem;
import static com.spotify.flo.freezer.EvaluatingContext.OUTPUT_SUFFIX;
import static com.spotify.flo.freezer.PersistingContext.cleanForFilename;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import com.spotify.flo.Task;
import com.spotify.flo.TaskContext;
import com.spotify.flo.TaskId;
import com.spotify.flo.context.AwaitingConsumer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class EvaluatingContextTest {

  @Rule
  public ExpectedException expect = ExpectedException.none();

  Path basePath;

  private PersistingContext persistingContext;
  private EvaluatingContext evaluatingContext;

  private static boolean calledInputCode;

  @Before
  public void setUp() throws Exception {
    basePath = Files.createTempDirectory("SpecificEvalTest");
    persistingContext = new PersistingContext(basePath, inmem());
    evaluatingContext = new EvaluatingContext(basePath, inmem());
  }

  @Test
  public void evaluatesPersistedTask() throws Exception {
    Task<String> task = singleTask("world");
    Path persistedPath = persist(task).get(task.id());

    AwaitingConsumer<String> await = AwaitingConsumer.create();
    evaluatingContext.<String>evaluateTaskFrom(persistedPath).consume(await);

    String output = await.awaitAndGet();
    assertThat(output, is("hello world"));
  }

  @Test
  public void persistsOutputOfEvaluatedTask() throws Exception {
    Task<String> task = singleTask("world");
    Path persistedPath = persist(task).get(task.id());

    AwaitingConsumer<String> await = AwaitingConsumer.create();
    final TaskContext.Value<String> stringValue =
        evaluatingContext.evaluateTaskFrom(persistedPath);
    stringValue.consume(await);
    stringValue.onFail(Throwable::printStackTrace);

    await.awaitAndGet();
    assertTrue(Files.exists(basePath.resolve(cleanForFilename(task.id()) + OUTPUT_SUFFIX)));
  }

  @Test
  public void failsIfTaskHasUnevaluatedInputs() throws Throwable {
    Task<String> task = downstreamTask("world");
    Map<TaskId, Path> persistedPaths = persist(task);

    AwaitingConsumer<Throwable> awaitFailure = AwaitingConsumer.create();
    Path downstreamPath = persistedPaths.get(task.id());
    final TaskContext.Value<String> stringValue =
        evaluatingContext.evaluateTaskFrom(downstreamPath);
    stringValue.onFail(awaitFailure);

    expect.expect(RuntimeException.class);
    expect.expectMessage("Output value for input task " + singleTask("world").id() + " not found");

    throw awaitFailure.awaitAndGet();
  }

  @Test
  public void readsPersistedInputValues() throws Exception {
    Task<String> task = downstreamTask("world");
    Map<TaskId, Path> persistedPaths = persist(task);

    evalUpstreamTask(persistedPaths);

    AwaitingConsumer<String> await = AwaitingConsumer.create();
    Path downstreamPath = persistedPaths.get(task.id());
    final TaskContext.Value<String> stringValue =
        evaluatingContext.evaluateTaskFrom(downstreamPath);
    stringValue.consume(await);
    stringValue.onFail(Throwable::printStackTrace);

    assertThat(await.awaitAndGet(), is("hello world!"));
  }

  @Test
  public void doesNotRunInputExpansionCode() throws Exception {
    Task<String> task = downstreamTask("world");
    Map<TaskId, Path> persistedPaths = persist(task);

    evalUpstreamTask(persistedPaths);

    calledInputCode = false;
    AwaitingConsumer<String> await = AwaitingConsumer.create();
    Path downstreamPath = persistedPaths.get(task.id());
    final TaskContext.Value<String> stringValue =
        evaluatingContext.evaluateTaskFrom(downstreamPath);
    stringValue.consume(await);
    stringValue.onFail(Throwable::printStackTrace);

    await.awaitAndGet();
    assertFalse(calledInputCode);
  }

  static Task<String> singleTask(String arg) {
    return Task.named("single", arg).ofType(String.class)
        .process(() -> "hello " + arg);
  }

  static Task<String> downstreamTask(String arg) {
    return Task.named("downstream", arg).ofType(String.class)
        .in(() -> {
          calledInputCode = true;
          return singleTask(arg);
        })
        .process((in) -> in + "!");
  }

  Map<TaskId, Path> persist(Task<String> task) {
    AwaitingConsumer<Throwable> awaitPersist = AwaitingConsumer.create();
    persistingContext.evaluate(task).onFail(awaitPersist); // persistingContext fails all evals

    return persistingContext.getFiles();
  }

  private void evalUpstreamTask(Map<TaskId, Path> persistedPaths) throws InterruptedException {
    AwaitingConsumer<String> awaitUpstream = AwaitingConsumer.create();
    Path upstreamPath = persistedPaths.get(singleTask("world").id());
    evaluatingContext.<String>evaluateTaskFrom(upstreamPath)
        .consume(awaitUpstream);
    awaitUpstream.awaitAndGet();
  }
}
