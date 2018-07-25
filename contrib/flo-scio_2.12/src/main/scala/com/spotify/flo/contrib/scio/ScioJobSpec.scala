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

class ScioJobSpec(private val taskId: TaskId) extends Serializable {

  private[scio] var _options: Option[F0[PipelineOptions]] = None
  private[scio] var _pipeline: F1[ScioContext, Any] = _
  private[scio] var _result: F2[ScioContext, ScioResult, Any] = _
  private[scio] var _success: F1[Any, _] = _

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

  def success[T](f: F1[Any, T]): T = {
    _success = f
    if (FloTesting.isTest) {
      runTest()
    } else {
      runProd()
    }
  }

  def runTest[U](): U = {
    val result = ScioOperator.mock().results.get(taskId)
    if (result.isDefined) {
      val value = _success.apply(result.get)
      return value.asInstanceOf[U]
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
        return _success.apply(result).asInstanceOf[U]
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

  def runProd[U](): U = {
    val sc = _options match {
      case None => ScioContext()
      case Some(options) => ScioContext(options.get())
    }
    val scioResult = sc.close().waitUntilDone()
    val result = _result.apply(sc, scioResult)
    _success.apply(result).asInstanceOf[U]
  }
}