/*-
 * -\-\-
 * flo-scio
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

package com.spotify.flo.contrib.scio

import com.spotify.flo.TaskBuilder.{F0, F1, F2}
import com.spotify.flo.TaskOperator
import com.spotify.scio.{ScioContext, ScioResult}
import org.apache.beam.sdk.options.PipelineOptions

class ScioJobSpec extends Serializable {

  private[scio] var _options: Option[F0[PipelineOptions]] = None
  private[scio] var _pipeline: F1[ScioContext, Any] = _
  private[scio] var _result: F2[ScioContext, ScioResult, Any] = _
  private[scio] var _success: F1[Any, Any] = _

  def options(f: F0[PipelineOptions]): ScioJobSpec = {
    _options = Some(f)
    this
  }

  def pipeline(f: F1[ScioContext, Any]): ScioJobSpec = {
    _pipeline = f
    this
  }

  def result(f: F2[ScioContext, ScioResult, Any]): ScioJobSpec = {
    _result = f
    this
  }

  def success[T](f: F1[Any, Any]): T = {
    _success = f
    throw new TaskOperator.SpecException(this)
  }
}