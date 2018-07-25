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
import com.spotify.flo.{FloTesting, TaskId}
import com.spotify.scio.{ScioContext, ScioResult}
import org.apache.beam.sdk.options.{ApplicationNameOptions, PipelineOptions, PipelineOptionsFactory}

class ScioJobSpec[R, S](private val taskId: TaskId,
                        private val _options: Option[F0[PipelineOptions]] = None,
                        private val _pipeline: F1[ScioContext, _] = null,
                        private val _result: F2[ScioContext, ScioResult, R] = null,
                        private val _success: F1[R, S] = null
                       ) extends Serializable {


  def options(options: F0[PipelineOptions]): ScioJobSpec[R, S] = {
    new ScioJobSpec[R, S](taskId, Some(options), _pipeline, _result, _success)
  }

  def pipeline(pipeline: F1[ScioContext, _]): ScioJobSpec[R, S] = {
    new ScioJobSpec[R, S](taskId, _options, pipeline, _result, _success)
  }

  def result[RN](result: F2[ScioContext, ScioResult, RN]): ScioJobSpec[RN, S] = {
    new ScioJobSpec[RN, S](taskId, _options, _pipeline, result, null)
  }

  def success[SN](success: F1[R, SN]): SN = {
    val spec = new ScioJobSpec[R, SN](taskId, _options, _pipeline, _result, success)
    spec.run()
  }

  private def run(): S = {
    if (_pipeline == null || _result == null || _success == null) {
      throw new IllegalStateException()
    }
    if (FloTesting.isTest) {
      runTest()
    } else {
      runProd()
    }
  }

  private def runTest(): S = {
    val result = ScioOperator.mock().results.get(taskId)
    if (result.isDefined) {
      return _success.apply(result.get.asInstanceOf[R])
    }

    val jobTest = ScioOperator.mock().jobTests.get(taskId)
    if (jobTest.isDefined) {
      jobTest.get.setUp()
      try {
        val sc = scioContextForTest(jobTest.get.testId)
        sc.options.as(classOf[ApplicationNameOptions]).setAppName(jobTest.get.testId)
        _pipeline.apply(sc)
        val scioResult = sc.close().waitUntilDone()
        val result = _result.apply(sc, scioResult)
        return _success.apply(result)
      } catch {
        case e: Exception => {
          e.printStackTrace()
          throw e
        }
      } finally {
        jobTest.get.tearDown()
      }
    }

    throw new AssertionError()
  }

  private def scioContextForTest[U](testId: String) = {
    // ScioContext.forTest does not seem to allow specifying testId
    val opts = PipelineOptionsFactory
      .fromArgs("--appName=" + testId)
      .as(classOf[PipelineOptions])
    ScioContext(opts)
  }

  private def runProd(): S = {
    val sc = _options match {
      case None => ScioContext()
      case Some(options) => ScioContext(options.get())
    }
    val scioResult = sc.close().waitUntilDone()
    val result = _result.apply(sc, scioResult)
    _success.apply(result)
  }
}