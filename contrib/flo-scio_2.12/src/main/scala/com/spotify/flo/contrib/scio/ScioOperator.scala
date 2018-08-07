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

import com.spotify.flo.{EvalContext, TaskId, TaskOperator, TestContext}
import com.spotify.scio.testing.JobTest
import com.spotify.scio.testing.JobTest.BeamOptions

import scala.collection.mutable

class ScioOperator[T] extends TaskOperator[ScioJobSpec.Provider, ScioJobSpec[_, T], T] {

  def provide(evalContext: EvalContext): ScioJobSpec.Provider = {
    new ScioJobSpec.Provider(evalContext.currentTask().get().id())
  }

  override def perform(o: ScioJobSpec[_, T], listener: TaskOperator.Listener): T = o.run(listener)
}

object ScioOperator {
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
