package io.rouz.flo.freezer;

import static io.rouz.flo.TaskContext.inmem;
import static io.rouz.flo.freezer.EvaluatingContext.OUTPUT_SUFFIX;
import static io.rouz.flo.freezer.PersistingContext.cleanForFilename;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import io.rouz.flo.Task;
import io.rouz.flo.TaskContext;
import io.rouz.flo.TaskId;
import io.rouz.flo.context.AwaitingConsumer;
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

  private Path basePath;
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

  private static Task<String> singleTask(String arg) {
    return Task.named("single", arg).ofType(String.class)
        .process(() -> "hello " + arg);
  }

  private static Task<String> downstreamTask(String arg) {
    return Task.named("downstream", arg).ofType(String.class)
        .in(() -> {
          calledInputCode = true;
          return singleTask(arg);
        })
        .process((in) -> in + "!");
  }

  private Map<TaskId, Path> persist(Task<String> task) {
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
