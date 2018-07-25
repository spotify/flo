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

import com.spotify.flo.TaskBuilder.F0
import com.spotify.flo.{EvalContext, TaskContextGeneric, TaskId, TestContext}
import com.spotify.scio.testing.JobTest
import com.spotify.scio.testing.JobTest.BeamOptions

import scala.collection.mutable

class ScioOperator extends TaskContextGeneric[ScioJobSpec[_, _]] {

  def provide(evalContext: EvalContext): ScioJobSpec[_, _] = {
    new ScioJobSpec(evalContext.currentTask().get().id())
  }
}

object ScioOperator {
  private val MOCK = TestContext.key(classOf[ScioOperator], "mock", () => new Mocking())

  def mock(): Mocking = {
    MOCK.get()
  }

  class Mocking extends TestContext.Value[Mocking] {
    private[scio] val results = mutable.Map[TaskId, Any]()
    private[scio] val jobTests = mutable.Map[TaskId, F0[JobTest.Builder]]()

    def result(id: TaskId, result: Any): Mocking = {
      results(id) = result
      this
    }

    def jobTest(id: TaskId)(setup: JobTest.Builder => Unit)(implicit bm: BeamOptions): Mocking = {
      jobTests(id) = () => {
        val b = JobTest(id.toString)
        setup(b)
        b
      }
      this
    }

    override def mergedOutput(other: Mocking): Mocking = this
    override def inputOnly(): Mocking = this
  }

  def apply(): ScioOperator = new ScioOperator()
}