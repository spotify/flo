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

import java.net.URLEncoder
import java.nio.charset.StandardCharsets.UTF_8

import com.spotify.flo.contrib.scio.ScioOperator.log
import com.spotify.flo.{EvalContext, FloTesting, TaskId, TaskOperator, TestContext}
import com.spotify.scio.ScioContext
import com.spotify.scio.testing.JobTest
import com.spotify.scio.testing.JobTest.BeamOptions
import org.apache.beam.runners.dataflow.DataflowPipelineJob
import org.apache.beam.sdk.options.{ApplicationNameOptions, PipelineOptions, PipelineOptionsFactory}
import org.apache.beam.sdk.{PipelineResult, PipelineRunner}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.{Failure, Try}

class ScioOperator[T] extends TaskOperator[ScioJobSpec.Provider[T], ScioJobSpec[_, T], T] {

  def provide(evalContext: EvalContext): ScioJobSpec.Provider[T] = {
    new ScioJobSpec.Provider(evalContext.currentTask().get().id())
  }

  override def perform(spec: ScioJobSpec[_, T], listener: TaskOperator.Listener): T = {
    spec.validate()
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

      // Set up pipeline
      val jobTest = jobTestSupplier()
      jobTest.setUp()
      val sc = scioContextForTest(spec.options(), jobTest.testId)
      spec.pipeline(sc)

      // Start job
      val scioResult = Try(sc.close())
      scioResult match {
        case Failure(t) => return spec.failure(t)
        case _ =>
      }

      // Wait for job to complete
      val done = Try(scioResult.get.waitUntilDone())
      done match {
        case Failure(t) => return spec.failure(t)
        case _ =>
      }

      // Handle result
      val result = Try(spec.result(sc, scioResult.get))
      result match {
        case Failure(t) => return spec.failure(t)
        case _ =>
      }

      // Success!
      jobTest.tearDown()
      return spec.success(result.get)
    }

    throw new AssertionError("Missing either mocked scio job result or JobTest, please set them up using either " +
      "ScioOperator.mock().result(...) or ScioOperator.mock().result().jobTest(...) before running the workflow")
  }

  private def scioContextForTest[U](options: PipelineOptions, testId: String) = {
    // ScioContext.forTest does not seem to allow specifying testId
    // always use DirectRunner on test mode
    val runner = Class.forName("org.apache.beam.runners.direct.DirectRunner")
      .asInstanceOf[Class[_ <: PipelineRunner[_ <: PipelineResult]]]
    options.setRunner(runner)
    options.as(classOf[ApplicationNameOptions]).setAppName(testId)
    val sc = ScioContext(options)
    if (!sc.isTest) {
      throw new AssertionError(s"Failed to create ScioContext for test with id ${testId}")
    }
    sc
  }

  private def runProd[R](spec: ScioJobSpec[R, T], listener: TaskOperator.Listener): T = {

    // Set up pipeline
    val sc = ScioContext(spec.options())
    spec.pipeline(sc)

    // Start job
    val scioResult = Try(sc.close())
    scioResult match {
      case Failure(t) => return spec.failure(t)
      case _ =>
    }

    // Report job id
    scioResult.get.internal match {
      case job: DataflowPipelineJob => reportDataflowJob(spec.taskId, job, listener)
      case _ =>
    }

    // Wait for job to complete
    val done = Try(scioResult.get.waitUntilDone())
    done match {
      case Failure(t) => return spec.failure(t)
      case _ =>
    }

    // Handle result
    val result = Try(spec.result(sc, scioResult.get))
    result match {
      case Failure(t) => return spec.failure(t)
      case _ =>
    }

    // Success!
    spec.success(result.get)
  }

  private def reportDataflowJob(taskId: TaskId, job: DataflowPipelineJob, listener: TaskOperator.Listener) {
    val url = dataflowJobMonitoringPageURL(job)
    val jobMeta = Map(
      "job-type" -> "dataflow",
      "job-id" -> job.getJobId,
      "project-id" -> job.getProjectId,
      "region" -> job.getRegion,
      "monitoring-page-url" -> url
    )
    log.info("Started scio job (dataflow): {}", jobMeta)
    listener.meta(taskId, jobMeta.asJava)
  }

  /**
    * From https://github.com/apache/beam/blob/master/runners/google-cloud-dataflow-java/src/main/java/org/apache/beam/runners/dataflow/util/MonitoringUtil.java
    */
  private def dataflowJobMonitoringPageURL(job: DataflowPipelineJob): String =
    String.format("https://console.cloud.google.com/dataflow/jobsDetail/locations/%s/jobs/%s?project=%s",
      URLEncoder.encode(job.getRegion, UTF_8.name),
      URLEncoder.encode(job.getJobId, UTF_8.name),
      URLEncoder.encode(job.getProjectId, UTF_8.name))

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
