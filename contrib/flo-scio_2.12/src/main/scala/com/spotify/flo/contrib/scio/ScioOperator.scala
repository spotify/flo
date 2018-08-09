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

import com.spotify.flo.contrib.scio.ScioOperator.log
import com.spotify.flo.{EvalContext, FloTesting, TaskId, TaskOperator, TestContext}
import com.spotify.scio.ScioContext
import com.spotify.scio.testing.JobTest
import com.spotify.scio.testing.JobTest.BeamOptions
import org.apache.beam.runners.dataflow.DataflowPipelineJob
import org.apache.beam.sdk.options.{ApplicationNameOptions, PipelineOptions, PipelineOptionsFactory}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable

class ScioOperator[T] extends TaskOperator[ScioJobSpec.Provider[T], ScioJobSpec[_, T], T] {

  def provide(evalContext: EvalContext): ScioJobSpec.Provider[T] = {
    new ScioJobSpec.Provider(evalContext.currentTask().get().id())
  }

  override def perform(spec: ScioJobSpec[_, T], listener: TaskOperator.Listener): T = {
    if (spec.pipeline == null || spec.result == null || spec.success == null) {
      throw new IllegalStateException()
    }
    if (FloTesting.isTest) {
      runTest(spec)
    } else {
      runProd(spec, listener)
    }
  }

  private def runTest[R](spec: ScioJobSpec[R, T]): T = {
    for (result <- ScioOperator.mock().results.get(spec.taskId)) {
      return spec.success(result.asInstanceOf[R])
    }

    for (jobTestSupplier <- ScioOperator.mock().jobTests.get(spec.taskId)) {
      val jobTest = jobTestSupplier()
      jobTest.setUp()
      val sc = scioContextForTest(jobTest.testId)
      sc.options.as(classOf[ApplicationNameOptions]).setAppName(jobTest.testId)
      spec.pipeline(sc)
      val scioResult = sc.close().waitUntilDone()
      val result = spec.result(sc, scioResult)
      jobTest.tearDown()
      return spec.success(result)
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

  private def runProd[R](spec: ScioJobSpec[R, T], listener: TaskOperator.Listener): T = {
    val sc = spec.options match {
      case None => ScioContext()
      case Some(options) => ScioContext(options())
    }
    spec.pipeline(sc)
    val scioResult = sc.close()
    scioResult.internal match {
      case job: DataflowPipelineJob => reportDataflowJobId(spec.taskId, job.getJobId, listener)
      case _ =>
    }
    scioResult.waitUntilDone()
    val result = spec.result(sc, scioResult)
    spec.success(result)
  }

  private def reportDataflowJobId(taskId: TaskId, jobId: String, listener: TaskOperator.Listener) {
    log.info("Started scio job (dataflow): {}", jobId)
    listener.meta(taskId, "dataflow-job-id", jobId);
  }
}

object ScioOperator {

  private val log: Logger = LoggerFactory.getLogger(classOf[ScioOperator[_]])

  private val MOCK = TestContext.key("mock", () => new Mocking())

  def mock(): Mocking = {
    MOCK.get()
  }

  class Mocking {
    private[scio] val results = mutable.Map[TaskId, Any]()
    private[scio] val jobTests = mutable.Map[TaskId, () => JobTest.Builder]()

    def result(id: TaskId, result: Any): Mocking = {
      results(id) = result
      this
    }

    def jobTest(id: TaskId)(setup: JobTest.Builder => JobTest.Builder)(implicit bm: BeamOptions): Mocking = {
      jobTests(id) = () => {
        // JobTest name may not contain dash
        val name = id.toString.replace("-", "_")
        val builder = JobTest(name)
        setup(builder)
        builder
      }
      this
    }
  }

  def apply[T](): ScioOperator[T] = new ScioOperator[T]()
}
