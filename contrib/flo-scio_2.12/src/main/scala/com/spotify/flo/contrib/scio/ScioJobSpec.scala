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

import com.spotify.flo.contrib.scio.ScioJobSpec.log
import com.spotify.flo.{FloTesting, TaskId}
import com.spotify.scio.{ScioContext, ScioResult}
import org.apache.beam.runners.dataflow.DataflowPipelineJob
import org.apache.beam.sdk.options.{ApplicationNameOptions, PipelineOptions, PipelineOptionsFactory}
import org.slf4j.{Logger, LoggerFactory}

class ScioJobSpec[R, S](private val taskId: TaskId,
                        private val _options: Option[() => PipelineOptions] = None,
                        private val _pipeline: ScioContext => Unit = null,
                        private val _result: (ScioContext, ScioResult) => R = null,
                        private val _success: R => S = null
                       ) extends Serializable {

  def options(options: () => PipelineOptions): ScioJobSpec[R, S] = {
    new ScioJobSpec(taskId, Some(options), _pipeline, _result, _success)
  }

  def pipeline(pipeline: ScioContext => Unit): ScioJobSpec[R, S] = {
    new ScioJobSpec(taskId, _options, pipeline, _result, _success)
  }

  def result[RN <: R](result: (ScioContext, ScioResult) => RN): ScioJobSpec[RN, S] = {
    new ScioJobSpec(taskId, _options, _pipeline, result, _success)
  }

  def success[SN](success: R => SN): SN = {
    val spec = new ScioJobSpec(taskId, _options, _pipeline, _result, success)
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
      return _success(result.get.asInstanceOf[R])
    }

    val jobTest = ScioOperator.mock().jobTests.get(taskId)
    if (jobTest.isDefined) {
      jobTest.get.setUp()
      try {
        val sc = scioContextForTest(jobTest.get.testId)
        sc.options.as(classOf[ApplicationNameOptions]).setAppName(jobTest.get.testId)
        _pipeline(sc)
        val scioResult = sc.close().waitUntilDone()
        val result = _result(sc, scioResult)
        return _success(result)
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
      case Some(options) => ScioContext(options())
    }
    _pipeline(sc)
    val scioResult = sc.close()
    scioResult.internal match {
      case job: DataflowPipelineJob => reportDataflowJobId(job.getJobId)
      case _ =>
    }
    scioResult.waitUntilDone()
    val result = _result(sc, scioResult)
    _success(result)
  }

  def reportDataflowJobId(getJobId: String) {
    log.info("Started scio job (dataflow): {}")
    // TODO: have some pluggable mechanism for reporting the job id
  }
}

object ScioJobSpec {
  private val log: Logger = LoggerFactory.getLogger(classOf[ScioJobSpec[_, _]])

  class Provider(taskId: TaskId) {
    def apply(): ScioJobSpec[Any, Any] = new ScioJobSpec(taskId)
  }
}
