/*-
 * -\-\-
 * flo runner
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

package com.spotify.flo.context;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;

import com.spotify.flo.Task;
import com.spotify.flo.context.InstrumentedContext.Listener;
import com.spotify.flo.context.InstrumentedContext.Listener.Phase;
import java.io.IOException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.slf4j.Logger;

@RunWith(MockitoJUnitRunner.class)
public class ChainedListenerTest {

  @Mock Listener first;
  @Mock Listener second;
  @Mock Logger logger;

  final IOException ioException = new IOException("TestException");

  Task<String> task = Task.named("test").ofType(String.class).process(() -> "hello");

  Listener sut;

  @Before
  public void setUp() throws Exception {
    sut = new ChainedListener(first, second, logger);
  }

  @Test
  public void testTask() throws Exception {
    sut.task(task);

    verify(first).task(task);
    verify(second).task(task);
  }

  @Test
  public void testHandlesExceptionOnTask() throws Exception {
    RuntimeException exception = new RuntimeException();
    doThrow(exception).when(first).task(any());
    sut.task(task);

    verify(logger).warn("Exception", exception);
    verify(second).task(task);
  }

  @Test
  public void testStatus() throws Exception {
    sut.status(task.id(), Phase.START);

    verify(first).status(task.id(), Phase.START);
    verify(second).status(task.id(), Phase.START);
  }

  @Test
  public void testHandlesExceptionOnStatus() throws Exception {
    RuntimeException exception = new RuntimeException();
    doThrow(exception).when(first).status(any(), any());
    sut.status(task.id(), Phase.START);

    verify(logger).warn("Exception", exception);
    verify(second).status(task.id(), Phase.START);
  }

  @Test(expected = IOException.class)
  public void shouldThrowExceptionOnClose() throws IOException {
    doThrow(ioException).when(first).close();
    verifyListenerClose();
  }

  @Test(expected = IOException.class)
  public void shouldThrowExceptionOnClose2() throws IOException {
    doThrow(ioException).when(second).close();
    verifyListenerClose();
  }

  @Test(expected = IOException.class)
  public void shouldThrowExceptionOnClose3() throws IOException {
    doThrow(ioException).when(first).close();
    doThrow(ioException).when(second).close();
    verifyListenerClose();
  }

  @Test
  public void shouldCloseWithoutException() throws IOException {
    verifyListenerClose();
  }

  private void verifyListenerClose() throws IOException {
    try {
      sut.close();
    } finally {
      verify(first).close();
      verify(second).close();
    }
  }
}
