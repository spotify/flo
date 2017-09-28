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

import com.spotify.flo.EvalContext.Value;
import java.io.Serializable;

/**
 * A function that evaluates some {@link Value} in a given {@link EvalContext}
 */
@FunctionalInterface
interface EvalClosure<T> extends Serializable {

  /**
   * Produces a {@link Value} in a given {@link EvalContext}.
   *
   * The produced {@link Value} should follow the semantics defined by the used {@link EvalContext}.
   *
   * @param evalContext  The context to evaluate in
   * @return a value
   */
  Value<T> eval(EvalContext evalContext);
}
