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

package com.spotify.flo.context;

import static com.spotify.flo.context.FloRunner.runTask;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mock;

import com.spotify.flo.Task;
import com.spotify.flo.freezer.Persisted;
import com.spotify.flo.status.NotReady;
import com.sun.tools.javac.util.List;
import java.util.ServiceLoader;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
@PrepareForTest(value = {FloRunner.class})
public class FloRunnerTest {

  final Task<String> FOO_TASK = Task.named("task").ofType(String.class)
      .process(() -> "foo");

  private ServiceLoader<TerminationHookFactory> terminationHookFactoryServiceLoader;
  private TerminationHookFactory exceptionalTerminationHookFactory;
  private TerminationHookFactory validTerminationHookFactory;
  private TerminationHookFactory nullTerminationHookFactory;
  private TerminationHook validTerminationHook;
  private TerminationHook exceptionalTerminationHook;

  @Before
  public void setUp() {
    PowerMockito.mockStatic(ServiceLoader.class);
    terminationHookFactoryServiceLoader = mock(ServiceLoader.class);

    when(ServiceLoader.load(eq(TerminationHookFactory.class))).thenCallRealMethod();
    when(ServiceLoader.load(eq(FloListenerFactory.class))).thenCallRealMethod();

    exceptionalTerminationHookFactory = mock(TerminationHookFactory.class);
    validTerminationHookFactory = mock(TerminationHookFactory.class);
    nullTerminationHookFactory = mock(TerminationHookFactory.class);

    validTerminationHook = mock(TerminationHook.class);
    doNothing().when(validTerminationHook).accept(any());

    exceptionalTerminationHook = mock(TerminationHook.class);
    doThrow(new RuntimeException("hook exception")).when(exceptionalTerminationHook).accept(any());

    when(exceptionalTerminationHookFactory.create())
        .thenThrow(new RuntimeException("exception exception"));
    when(nullTerminationHookFactory.create()).thenReturn(null);
    when(validTerminationHookFactory.create()).thenReturn(validTerminationHook);
  }

  @Test
  public void nonBlockingRunnerDoesNotBlock() {
    final AtomicBoolean hasHappened = new AtomicBoolean(false);
    final CountDownLatch latch = new CountDownLatch(1);
    final Task<Void> task = Task.named("task").ofType(Void.class)
        .process(() -> {
          try {
            latch.await();
            hasHappened.set(true);
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
          return null;
        });

    runTask(task);

    assertThat(hasHappened.get(), is(false));

    latch.countDown();
  }

  @Test
  public void blockingRunnerBlocks() {
    final AtomicBoolean hasHappened = new AtomicBoolean();
    final Task<Void> task = Task.named("task").ofType(Void.class)
        .process(() -> {
          try {
            Thread.sleep(10);
            hasHappened.set(true);
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
          return null;
        });

    runTask(task).waitAndExit(status -> { });

    assertThat(hasHappened.get(), is(true));
  }

  @Test
  public void valueIsPassedInFuture() throws Exception {
    final String result = runTask(FOO_TASK).future().get(1, TimeUnit.SECONDS);

    assertThat(result, is("foo"));
  }

  @Test
  public void exceptionsArePassed() throws Exception {
    final RuntimeException expectedException = new RuntimeException("foo");

    final Task<String> task = Task.named("foo").ofType(String.class)
        .process(() -> {
          throw expectedException;
        });

    Throwable exception = null;
    try {
      runTask(task).value();
    } catch (ExecutionException e) {
      exception = e.getCause();
    }
    assertThat(exception, is(expectedException));
  }

  @Test
  public void persistedExitsZero() throws Exception {
    final Task<Void> task = Task.named("persisted").ofType(Void.class)
        .process(() -> {
          throw new Persisted();
        });

    AtomicInteger status = new AtomicInteger(1);

    runTask(task).waitAndExit(status::set);

    assertThat(status.get(), is(0));
  }

  @Test
  public void valuesCanBeWaitedOn() throws Exception {
    final String result = runTask(FOO_TASK).value();

    assertThat(result, is("foo"));
  }

  @Test
  public void notReadyExitsTwenty() throws Exception {
    final Task<String> task = Task.named("task").ofType(String.class)
        .process(() -> {
          throw new NotReady();
        });

    AtomicInteger status = new AtomicInteger();

    runTask(task).waitAndExit(status::set);

    assertThat(status.get(), is(20));
  }

  @Test
  public void exceptionsExitNonZero() throws Exception {
    final Task<String> task = Task.named("task").ofType(String.class)
        .process(() -> {
          throw new RuntimeException("this task should throw");
        });

    AtomicInteger status = new AtomicInteger();

    runTask(task).waitAndExit(status::set);

    assertThat(status.get(), is(1));
  }

  @Test
  public void handleInvalidTerminationHookFactories() throws Exception {
    when(ServiceLoader.load(eq(TerminationHookFactory.class))).thenReturn(
        terminationHookFactoryServiceLoader);
    when(terminationHookFactoryServiceLoader.iterator()).thenReturn(
        List.of(exceptionalTerminationHookFactory, nullTerminationHookFactory).iterator());

    AtomicInteger status = new AtomicInteger();
    runTask(FOO_TASK).waitAndExit(status::set);

    verify(exceptionalTerminationHookFactory, times(1)).create();
    verify(nullTerminationHookFactory, times(1)).create();

    assertThat(status.get(), is(0));
  }

  @Test
  public void verifyCallToValidTerminationHookFactories() throws Exception {
    when(ServiceLoader.load(eq(TerminationHookFactory.class))).thenReturn(
        terminationHookFactoryServiceLoader);
    when(terminationHookFactoryServiceLoader.iterator()).thenReturn(
        List.of(validTerminationHookFactory).iterator());

    AtomicInteger status = new AtomicInteger();
    runTask(FOO_TASK).waitAndExit(status::set);

    verify(validTerminationHookFactory, times(1)).create();
    verify(validTerminationHook, times(1)).accept(0);

    assertThat(status.get(), is(0));
  }

  @Test
  public void handleExceptionalTerminationHooks() throws Exception {
    when(ServiceLoader.load(eq(TerminationHookFactory.class))).thenReturn(
        terminationHookFactoryServiceLoader);
    when(terminationHookFactoryServiceLoader.iterator()).thenReturn(
        List.of(validTerminationHookFactory).iterator());
    when(validTerminationHookFactory.create()).thenReturn(exceptionalTerminationHook);

    AtomicInteger status = new AtomicInteger();
    runTask(FOO_TASK).waitAndExit(status::set);

    verify(validTerminationHookFactory, times(1)).create();
    verify(exceptionalTerminationHook, times(1)).accept(0);

    assertThat(status.get(), is(0));
  }
}
