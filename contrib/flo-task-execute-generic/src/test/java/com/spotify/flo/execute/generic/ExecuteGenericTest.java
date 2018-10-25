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

package com.spotify.flo.execute.generic;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import org.junit.Test;

public class ExecuteGenericTest {

  @Test
  public void testExecute() throws InterruptedException, ExecutionException, TimeoutException, IOException {
    ExecuteGeneric.main(
        "gs://dano-test/staging/executions/execution-2018-10-23-0b42acb8-90f4-410d-8112-e7af2414446d/manifest.json",
        "foo()#a64ce3d3");
  }
}