/*
 * Copyright 2011-2017 Asakusa Framework Team.
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
package com.asakusafw.spark.runtime
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

@RunWith(classOf[JUnitRunner])
class DirectOutputSetupSpecTest extends DirectOutputSetupSpec

class DirectOutputSetupSpec
  extends FlatSpec
  with SparkForAll
  with JobContextSugar
  with RoundContextSugar
  with TempDirForAll {

  import DirectOutputSetupSpec._

  behavior of classOf[DirectOutputSetup].getSimpleName

  private var root: File = _

  override def configure(conf: SparkConf): SparkConf = {
    root = createTempDirectoryForAll("directio-").toFile()
    conf.setHadoopConf("com.asakusafw.directio.test", classOf[HadoopDataSource].getName)
    conf.setHadoopConf("com.asakusafw.directio.test.path", "test")
    conf.setHadoopConf("com.asakusafw.directio.test.fs.path", root.getAbsolutePath)
  }

  it should "delete simple" in {
    implicit val jobContext = newJobContext(sc)

    val file = new File(root, "out1/testing.bin")
    file.getParentFile.mkdirs()
    file.createNewFile()

    val setup = new Setup(Set(("id", "test/out1", Seq("*.bin"))))
    val rc = newRoundContext()

    Await.result(setup.perform(rc), Duration.Inf)

    assert(file.exists() === false)
  }

  it should "not delete out of scope" in {
    implicit val jobContext = newJobContext(sc)

    val file = new File(root, "out2/testing.bin")
    file.getParentFile.mkdirs()
    file.createNewFile()

    val setup = new Setup(Set(("id", "test/out2", Seq("*.txt"))))
    val rc = newRoundContext()

    Await.result(setup.perform(rc), Duration.Inf)

    assert(file.exists() === true)
  }
}

object DirectOutputSetupSpec {

  class Setup(
    val specs: Set[(String, String, Seq[String])])(
      implicit jobContext: JobContext)
    extends DirectOutputSetup with CacheOnce[RoundContext, Future[Unit]]
}
