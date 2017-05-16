package io.rouz.flo.freezer;

import static io.rouz.flo.freezer.EvaluatingContext.OUTPUT_SUFFIX;
import static io.rouz.flo.freezer.PersistingContext.cleanForFilename;
import static org.junit.Assert.assertTrue;

import io.rouz.flo.Task;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.Before;
import org.junit.Test;

public class TaskRunnerEntrypointTest {

  private EvaluatingContextTest subTest = new EvaluatingContextTest();

  @Before
  public void setUp() throws Exception {
    subTest.setUp();
  }

  @Test
  public void evalEntrypointShouldProduceOutputOfTask() throws Exception {
    Task<String> task = EvaluatingContextTest.singleTask("world");
    Path persistedPath = subTest.persist(task).get(task.id());

    TaskRunnerEntrypoint.main(new String[]{persistedPath.toUri().toString()});

    assertTrue(Files.exists(subTest.basePath.resolve(cleanForFilename(task.id()) + OUTPUT_SUFFIX)));
  }
}
