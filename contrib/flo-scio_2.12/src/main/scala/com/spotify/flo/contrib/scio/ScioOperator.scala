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

import com.spotify.flo.{EvalContext, FloTesting, Task, TaskId, TaskOperator, TestContext}
import com.spotify.scio.ScioContext
import com.spotify.scio.testing.JobTest
import com.spotify.scio.testing.JobTest.BeamOptions
import org.apache.beam.sdk.options.{ApplicationNameOptions, PipelineOptions, PipelineOptionsFactory}

import scala.collection.mutable

class ScioOperator extends TaskOperator[ScioJobSpec] {

  def provide(evalContext: EvalContext): ScioJobSpec = {
    new ScioJobSpec()
  }

  def runTest[U](task: Task[_], spec: ScioJobSpec): U = {
    val result = ScioOperator.mock().results.get(task.id())
    if (result.isDefined) {
      val value = spec._success.apply(result.get)
      return value.asInstanceOf[U]
    }

    val jobTest = ScioOperator.mock().jobTests.get(task.id())
    if (jobTest.isDefined) {
      jobTest.get.setUp()
      try {
        val sc = scioContextForTest(jobTest.get.testId)
        sc.options.as(classOf[ApplicationNameOptions]).setAppName(jobTest.get.testId)
        spec._pipeline.apply(sc)
        val scioResult = sc.close().waitUntilDone()
        val result = spec._result.apply(sc, scioResult)
        return spec._success.apply(result).asInstanceOf[U]
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

  def runProd[U](task: Task[_], spec: ScioJobSpec): U = {
    val sc = spec._options match {
      case None => ScioContext()
      case Some(options) => ScioContext(options.get())
    }
    val scioResult = sc.close().waitUntilDone()
    val result = spec._result.apply(sc, scioResult)
    spec._success.apply(result).asInstanceOf[U]
  }

  def run[U](task: Task[_], spec: ScioJobSpec): U = {
    if (FloTesting.isTest) {
      runTest(task, spec)
    } else {
      runProd(task, spec)
    }
  }
}

object ScioOperator {
  private val MOCK = TestContext.key("mock", () => new Mocking())

  def mock(): Mocking = {
    MOCK.get()
  }

  class Mocking {
    private[scio] val results = mutable.Map[TaskId, Any]()
    private[scio] val jobTests = mutable.Map[TaskId, JobTest.Builder]()

    def result(id: TaskId, result: Any): Mocking = {
      results(id) = result
      this
    }

    def jobTest(id: TaskId, jobTestBuilder: JobTest.Builder): Mocking = {
      jobTests(id) = jobTestBuilder
      this
    }

    def jobTest(id: TaskId)(implicit bm: BeamOptions): JobTest.Builder = {
      val t = JobTest(id.toString)
      jobTest(id, t)
      t
    }
  }
}