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
import com.spotify.flo.{FloTesting, TaskId, TaskOperator}
import com.spotify.scio.{ScioContext, ScioResult}
import org.apache.beam.runners.dataflow.DataflowPipelineJob
import org.apache.beam.sdk.options.{ApplicationNameOptions, PipelineOptions, PipelineOptionsFactory}
import org.slf4j.{Logger, LoggerFactory}

class ScioJobSpec[R, S](private val taskId: TaskId,
                        private val _options: Option[() => PipelineOptions] = None,
                        private val _pipeline: ScioContext => Unit = null,
                        private val _result: (ScioContext, ScioResult) => R = null,
                        private val _success: R => S = null
                       ) extends TaskOperator.OperatorSpec[S] with Serializable {

  def options(options: () => PipelineOptions): ScioJobSpec[R, S] = {
    new ScioJobSpec(taskId, Some(options), _pipeline, _result, _success)
  }

  def pipeline(pipeline: ScioContext => Unit): ScioJobSpec[R, S] = {
    new ScioJobSpec(taskId, _options, pipeline, _result, _success)
  }

  def result[RN <: R](result: (ScioContext, ScioResult) => RN): ScioJobSpec[RN, S] = {
    new ScioJobSpec(taskId, _options, _pipeline, result, _success)
  }

  def success[SN](success: R => SN): ScioJobSpec[R, SN] = {
    new ScioJobSpec(taskId, _options, _pipeline, _result, success)
  }

  private[scio] override def run(listener: TaskOperator.Listener): S = {
    if (_pipeline == null || _result == null || _success == null) {
      throw new IllegalStateException()
    }
    if (FloTesting.isTest) {
      runTest()
    } else {
      runProd(listener)
    }
  }

  private def runTest(): S = {
    for (result <- ScioOperator.mock().results.get(taskId)) {
      return _success(result.asInstanceOf[R])
    }

    for (jobTestSupplier <- ScioOperator.mock().jobTests.get(taskId)) {
      val jobTest = jobTestSupplier()
      jobTest.setUp()
      val sc = scioContextForTest(jobTest.testId)
      sc.options.as(classOf[ApplicationNameOptions]).setAppName(jobTest.testId)
      _pipeline(sc)
      val scioResult = sc.close().waitUntilDone()
      val result = _result(sc, scioResult)
      jobTest.tearDown()
      return _success(result)
    }

    throw new AssertionError()
  }

  private def scioContextForTest[U](testId: String) = {
    // ScioContext.forTest does not seem to allow specifying testId
    val opts = PipelineOptionsFactory
      .fromArgs("--appName=" + testId)
      .as(classOf[PipelineOptions])
    val sc = ScioContext(opts)
    if (!sc.isTest) {
      throw new AssertionError(s"Failed to create ScioContext for test with id ${testId}")
    }
    sc
  }

  private def runProd(listener: TaskOperator.Listener): S = {
    val sc = _options match {
      case None => ScioContext()
      case Some(options) => ScioContext(options())
    }
    _pipeline(sc)
    val scioResult = sc.close()
    scioResult.internal match {
      case job: DataflowPipelineJob => reportDataflowJobId(job.getJobId, listener)
      case _ =>
    }
    scioResult.waitUntilDone()
    val result = _result(sc, scioResult)
    _success(result)
  }

  def reportDataflowJobId(jobId: String, listener: TaskOperator.Listener) {
    log.info("Started scio job (dataflow): {}", jobId)
    listener.meta(taskId, "dataflow-job-id", jobId);
  }
}

object ScioJobSpec {
  private val log: Logger = LoggerFactory.getLogger(classOf[ScioJobSpec[_, _]])

  class Provider[Z](taskId: TaskId) {
    def apply(): ScioJobSpec[Any, Z] = new ScioJobSpec(taskId)
  }
}
