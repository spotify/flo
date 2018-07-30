/*-
 * -\-\-
 * Flo Workflow Definition
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

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.spotify.flo.EvalContext;
import com.spotify.flo.EvalContext.Value;
import com.spotify.flo.Task;
import com.spotify.flo.TaskBuilder.F0;
import com.spotify.flo.TaskOperator;
import com.spotify.flo.TaskOperator.OperationException;
import java.util.concurrent.CompletableFuture;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;


@RunWith(MockitoJUnitRunner.class)
public class OperatingContextTest {

  @Mock OperationException operationException;
  @Mock F0<String> spec;
  @Mock TaskOperator<F0<String>> operator;

  private final EvalContext operatingContext = OperatingContext.composeWith(EvalContext.sync());

  private Task<String> task;

  @Before
  public void setUp() {
    when(operator.provide(any())).thenReturn(spec);
    task = Task.named("task")
        .ofType(String.class)
        .context(operator)
        .process(F0::get);
  }

  @Test
  public void shouldRunOperationException() throws Exception {
    when(spec.get()).thenThrow(operationException);
    when(operationException.run()).thenReturn("foo");
    final String result = future(operatingContext.evaluate(task)).get(30, SECONDS);
    assertThat(result, is("foo"));
    verify(operationException).run();
  }

  @Test
  public void shouldReturnValueDirectly() throws Exception {
    when(spec.get()).thenReturn("bar");
    final String result = future(operatingContext.evaluate(task)).get(30, SECONDS);
    assertThat(result, is("bar"));
    verify(operationException, never()).run();
  }

  private CompletableFuture<String> future(Value<String> v) {
    final CompletableFuture<String> future = new CompletableFuture<>();
    v.consume(future::complete);
    v.onFail(future::completeExceptionally);
    return future;
  }
}