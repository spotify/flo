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

import com.spotify.flo.TaskId
import com.spotify.scio.{ScioContext, ScioResult}
import org.apache.beam.sdk.options.{PipelineOptions, PipelineOptionsFactory}

class ScioJobSpec[R, S](private[scio] val taskId: TaskId,
                        private[scio] val options: () => PipelineOptions = () => PipelineOptionsFactory.create(),
                        private[scio] val pipeline: ScioContext => Unit = null,
                        private[scio] val result: (PipelineOptions, ScioResult) => R = null,
                        private[scio] val success: R => S = null,
                        private[scio] val failure: Throwable => S = (t: Throwable) => { throw t }
                       ) extends Serializable {

  def options(options: () => PipelineOptions): ScioJobSpec[R, S] = {
    require(options != null)
    new ScioJobSpec(taskId, options, pipeline, result, success, failure)
  }

  def pipeline(pipeline: ScioContext => Unit): ScioJobSpec[R, S] = {
    require(pipeline != null)
    new ScioJobSpec(taskId, options, pipeline, result, success, failure)
  }

  def result[RN <: R](result: (PipelineOptions, ScioResult) => RN): ScioJobSpec[RN, S] = {
    require(result != null)
    new ScioJobSpec(taskId, options, pipeline, result, success, failure)
  }

  def success(success: R => S): ScioJobSpec[R, S] = {
    require(success != null)
    new ScioJobSpec(taskId, options, pipeline, result, success, failure)
  }

  def failure(failure: Throwable => S): ScioJobSpec[R, S] = {
    require(failure != null)
    new ScioJobSpec(taskId, options, pipeline, result, success, failure)
  }

  private[scio] def validate(): Unit = {
    require(options != null)
    require(pipeline != null)
    require(result != null)
    require(success != null)
    require(failure != null)
  }
}

object ScioJobSpec {
  class Provider[Z](taskId: TaskId) {
    def apply(): ScioJobSpec[Any, Z] = new ScioJobSpec(taskId)
  }
}
