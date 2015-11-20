/*
 * Copyright 2011-2015 Asakusa Framework Team.
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
import org.scalatest.fixture.FlatSpec
import org.scalatest.junit.JUnitRunner

import java.io.{ DataInput, DataOutput, File }

import scala.collection.JavaConversions._
import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration

import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{ NullWritable, Writable }
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.spark.{ SparkConf, SparkContext }

import com.asakusafw.bridge.stage.StageInfo
import com.asakusafw.runtime.compatibility.JobCompatibility
import com.asakusafw.runtime.model.DataModel
import com.asakusafw.runtime.stage.StageConstants.EXPR_EXECUTION_ID
import com.asakusafw.runtime.stage.input.TemporaryInputFormat
import com.asakusafw.runtime.value.IntOption
import com.asakusafw.spark.runtime.TempDirForEach
import com.asakusafw.spark.runtime.fixture.SparkForAll
import com.asakusafw.spark.runtime.rdd._

abstract class OutputSpec extends FlatSpec with SparkForAll {

  import OutputSpec._

  def readResult(path: String, rc: RoundContext): Seq[Int] = {
    val job = JobCompatibility.newJob(rc.hadoopConf.value)

    val stageInfo = StageInfo.deserialize(job.getConfiguration.get(StageInfo.KEY_NAME))
    FileInputFormat.setInputPaths(job, new Path(stageInfo.resolveUserVariables(path + "/part-*")))

    sc.newAPIHadoopRDD(
      job.getConfiguration,
      classOf[TemporaryInputFormat[Foo]],
      classOf[NullWritable],
      classOf[Foo]).map(_._2.id.get).collect.toSeq.sorted
  }
}

@RunWith(classOf[JUnitRunner])
class TemporaryOutputSpecTest extends TemporaryOutputSpec

class TemporaryOutputSpec extends OutputSpec with RoundContextSugar with TempDirForEach {

  import OutputSpec._

  behavior of classOf[TemporaryOutput[_]].getSimpleName

  for {
    numSlices <- Seq(None, Some(8), Some(4))
  } {
    val conf = s"numSlices = ${numSlices}"

    it should s"output: [${conf}]" in { implicit sc =>
      val tmpDir = createTempDirectoryForEach("test-").toFile.getAbsolutePath
      val path = new File(tmpDir, s"foos-${EXPR_EXECUTION_ID}").getAbsolutePath

      val source =
        new ParallelCollectionSource(Input, (0 until 100))("input")
          .map(Input)(Foo.intToFoo)
      val output = new Temporary.Output((source, Input))(path, "output")

      val rc = newRoundContext()

      Await.result(output.submitJob(rc), Duration.Inf)

      val result = readResult(path, rc)
      assert(result.size === 100)
      assert(result === (0 until 100))
    }

    it should s"output multiple prevs: [${conf}]" in { implicit sc =>
      val tmpDir = createTempDirectoryForEach("test-").toFile.getAbsolutePath
      val path = new File(tmpDir, s"foos-${EXPR_EXECUTION_ID}").getAbsolutePath

      val source1 =
        new ParallelCollectionSource(Input, (0 until 50), numSlices)("input")
          .map(Input)(Foo.intToFoo)
      val source2 =
        new ParallelCollectionSource(Input, (50 until 100), numSlices)("input")
          .map(Input)(Foo.intToFoo)
      val output = new Temporary.Output(Seq((source1, Input), (source2, Input)))(path, "output")

      val rc = newRoundContext()

      Await.result(output.submitJob(rc), Duration.Inf)

      val result = readResult(path, rc)
      assert(result.size === 100)
      assert(result === (0 until 100))
    }
  }
}

@RunWith(classOf[JUnitRunner])
class TemporaryOutputWithParallelismSpecTest extends TemporaryOutputWithParallelismSpec

class TemporaryOutputWithParallelismSpec extends TemporaryOutputSpec {

  override def configure(conf: SparkConf): SparkConf = {
    conf.set("spark.default.parallelism", 8.toString)
  }
}

object OutputSpec {

  class Foo extends DataModel[Foo] with Writable {

    val id = new IntOption()

    override def reset(): Unit = {
      id.setNull()
    }
    override def copyFrom(other: Foo): Unit = {
      id.copyFrom(other.id)
    }
    override def readFields(in: DataInput): Unit = {
      id.readFields(in)
    }
    override def write(out: DataOutput): Unit = {
      id.write(out)
    }
  }

  object Foo {

    def intToFoo: Int => (_, Foo) = {

      lazy val foo = new Foo()

      { i =>
        foo.id.modify(i)
        (NullWritable.get, foo)
      }
    }
  }

  val Input = BranchKey(0)

  object Temporary {

    class Output(
      prevs: Seq[(Source, BranchKey)])(
        val path: String,
        val label: String)(
          implicit sc: SparkContext)
      extends TemporaryOutput[Foo](prevs) {

      def this(
        prev: (Source, BranchKey))(
          path: String,
          label: String)(
            implicit sc: SparkContext) = this(Seq(prev))(path, label)
    }
  }
}
