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

import static com.spotify.flo.freezer.EvaluatingContext.OUTPUT_SUFFIX;
import static com.spotify.flo.freezer.PersistingContext.cleanForFilename;
import static org.junit.Assert.assertTrue;

import com.spotify.flo.Task;
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
