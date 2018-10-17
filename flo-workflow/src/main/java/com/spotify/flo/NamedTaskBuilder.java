/*-
 * -\-\-
 * Flo Workflow Definition
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

package com.spotify.flo;

import io.vavr.Tuple2;
import io.vavr.Tuple3;

/**
 * The initial part of the {@link TaskBuilder} api which only holds the task name.
 * See {@link Task#named(String, Object...)}.
 */
public interface NamedTaskBuilder {
  <Z> TaskBuilder<Z> ofType(Class<Z> type);
  <Z> TaskBuilder<Z> ofType(TypeReference<Z> type);
  <Z1, Z2> TaskBuilder<Tuple2<Z1, Z2>> ofType(Class<Z1> t1, Class<Z2> t2);
  <Z1, Z2, Z3> TaskBuilder<Tuple3<Z1, Z2, Z3>> ofType(Class<Z1> t1, Class<Z2> t2, Class<Z3> t3);
}
