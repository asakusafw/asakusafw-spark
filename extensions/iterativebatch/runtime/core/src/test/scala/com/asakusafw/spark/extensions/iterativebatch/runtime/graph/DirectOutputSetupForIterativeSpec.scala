/*
 * Copyright 2011-2019 Asakusa Framework Team.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.asakusafw.spark.extensions.iterativebatch.runtime
package graph

import org.junit.runner.RunWith
import org.scalatest.FlatSpec
import org.scalatest.junit.JUnitRunner

import java.io.File

import scala.concurrent.{ Await, Future }
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration

import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkConf

import com.asakusafw.runtime.directio.hadoop.HadoopDataSource
import com.asakusafw.spark.runtime._
import com.asakusafw.spark.runtime.graph.{ CacheOnce, DirectOutputSetup }

@RunWith(classOf[JUnitRunner])
class DirectOutputSetupForIterativeSpecTest extends DirectOutputSetupForIterativeSpec

class DirectOutputSetupForIterativeSpec
  extends FlatSpec
  with SparkForAll
  with JobContextSugar
  with RoundContextSugar
  with TempDirForAll {

  import DirectOutputSetupForIterativeSpec._

  behavior of classOf[DirectOutputSetupForIterative].getSimpleName

  private var root: File = _

  override def configure(conf: SparkConf): SparkConf = {
    root = createTempDirectoryForAll("directio-").toFile()
    conf.setHadoopConf("com.asakusafw.directio.test", classOf[HadoopDataSource].getName)
    conf.setHadoopConf("com.asakusafw.directio.test.path", "test")
    conf.setHadoopConf("com.asakusafw.directio.test.fs.path", root.getAbsolutePath)
  }

  it should "delete simple" in {
    implicit val jobContext = newJobContext(sc)

    val rounds = 0 to 1
    val files = rounds.map { round =>
      val file = new File(root, s"out1_${round}/testing.bin")
      file.getParentFile.mkdirs()
      file.createNewFile()
      file
    }

    val setup = new SetupOnce(new Setup(Set(("id", "test/out1_${round}", Seq("*.bin")))))
    val origin = newRoundContext()
    val rcs = rounds.map { round =>
      newRoundContext(
        stageId = s"round_${round}",
        batchArguments = Map("round" -> round.toString))
    }

    assert(files.forall(_.exists()) === true)

    Await.result(setup.perform(origin, rcs), Duration.Inf)

    assert(files.exists(_.exists()) === false)
  }

  it should "not delete out of scope" in {
    implicit val jobContext = newJobContext(sc)

    val rounds = 0 to 1
    val files = rounds.map { round =>
      val file = new File(root, s"out2_${round}/testing.bin")
      file.getParentFile.mkdirs()
      file.createNewFile()
      file
    }

    val setup = new SetupOnce(new Setup(Set(("id", "test/out2_${round}", Seq("*.txt")))))
    val origin = newRoundContext()
    val rcs = rounds.map { round =>
      newRoundContext(
        stageId = s"round_${round}",
        batchArguments = Map("round" -> round.toString))
    }

    assert(files.forall(_.exists()) === true)

    Await.result(setup.perform(origin, rcs), Duration.Inf)

    assert(files.forall(_.exists()) === true)
  }

  it should "not delete out of scope round" in {
    implicit val jobContext = newJobContext(sc)

    val rounds = 0 to 1
    val files = rounds.map { round =>
      val file = new File(root, s"out3_${round}/testing.bin")
      file.getParentFile.mkdirs()
      file.createNewFile()
      file
    }

    val setup = new SetupOnce(new Setup(Set(("id", "test/out3_${round}", Seq("*.bin")))))
    val origin = newRoundContext()
    val rcs = rounds.map { round =>
      newRoundContext(
        stageId = s"round_${round}",
        batchArguments = Map("round" -> round.toString))
    }

    assert(files.forall(_.exists()) === true)

    Await.result(setup.perform(origin, Seq(rcs.head)), Duration.Inf)

    assert(files.head.exists() === false)
    assert(files.tail.forall(_.exists()) === true)

    Await.result(setup.perform(origin, rcs.tail), Duration.Inf)

    assert(files.exists(_.exists()) === false)
  }
}

object DirectOutputSetupForIterativeSpec {

  class Setup(
    val specs: Set[(String, String, Seq[String])])(
      implicit jobContext: JobContext)
    extends DirectOutputSetup with CacheAlways[RoundContext, Future[Unit]]

  class SetupOnce(
    setup: DirectOutputSetup)(
      implicit jobContext: JobContext)
    extends DirectOutputSetupForIterative(setup)
    with CacheAlways[Seq[RoundContext], Future[Unit]]
}
