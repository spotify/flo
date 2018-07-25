/*-
 * -\-\-
 * Flo BigQuery
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

import java.util.concurrent.TimeUnit

import com.spotify.flo
import com.spotify.flo._
import com.spotify.flo.context.FloRunner
import com.spotify.flo.contrib.scio.ScioOperatorTest._
import com.spotify.scio.ScioMetrics
import com.spotify.scio.testing.{PipelineSpec, TextIO}
import org.apache.beam.sdk.metrics.Counter
import org.scalatest._

class ScioOperatorTest extends PipelineSpec with Matchers {

  it should "be able to run a scio job with mocked result" in {
    val result = flo.test(() => {
      ScioOperator.mock().result(task.id(), 42L)
      FloRunner.runTask(task).future().get(30, TimeUnit.SECONDS)
    })
    result shouldBe "lines: 42"
  }

  it should "be able to run a scio job with JobTest" in {
    val result = flo.test(() => {
      ScioOperator.mock().jobTest(task.id()) {
        _.input(TextIO("input.txt"), Seq("foo", "bar", "baz"))
          .output(TextIO("output.txt")) {
            actual => {}
            // TODO: get this output verification working
            //          actual => actual should containInAnyOrder Seq("foo", "bar", "baz")
          }
      }
      FloRunner.runTask(task).future().get(30, TimeUnit.SECONDS)
    })
    result shouldBe "lines: 3"
  }
}

object ScioOperatorTest {
  val linesCounter: Counter = ScioMetrics.counter[ScioOperatorTest]("count")

  val task = defTaskNamed[String]("foobar")
    .context(ScioOperator())
    .process { job =>
      job.pipeline(sc => {
        sc.textFile("input.txt")
          .map(row => {
            linesCounter.inc()
          })
          .saveAsTextFile("output.txt")
      })
        .result((sc, result) => {
          val lines = result.counter(linesCounter).committed match {
            case Some(n) => n
            case _ => 0
          }
          if (lines < 3) throw new AssertionError("too few rows") else lines
        })
        .success(r => {
          "lines: " + r.toString
        })
    }
}
