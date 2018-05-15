/*-
 * -\-\-
 * flo-styx
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

package com.spotify.flo.contrib.styx;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;

import com.spotify.flo.context.TerminationHookFactory;
import com.typesafe.config.Config;
import org.junit.Before;
import org.junit.Test;

public class TerminationLoggingFactoryTest {
  private TerminationHookFactory terminationHookFactory;
  
  @Before
  public void setUp() {
    terminationHookFactory = new TerminationLoggingFactory();
  }

  @Test
  public void shouldCreateInstance() {
    assertThat(terminationHookFactory.create(mock(Config.class)), is(notNullValue()));
  }
}
