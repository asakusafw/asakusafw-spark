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
package com.asakusafw.spark.extensions.iterativebatch.runtime
package flow

import org.junit.runner.RunWith
import org.scalatest.fixture.FlatSpec
import org.scalatest.junit.JUnitRunner

import java.io.{ DataInput, DataOutput, File }

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{ NullWritable, Writable }
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.spark.{ Partitioner, SparkContext }
import org.apache.spark.broadcast.{ Broadcast => Broadcasted }

import com.asakusafw.runtime.compatibility.JobCompatibility
import com.asakusafw.runtime.model.DataModel
import com.asakusafw.runtime.stage.input.TemporaryInputFormat
import com.asakusafw.runtime.stage.output.TemporaryOutputFormat
import com.asakusafw.runtime.value.IntOption
import com.asakusafw.spark.runtime.TempDirForEach
import com.asakusafw.spark.runtime.aggregation.Aggregation
import com.asakusafw.spark.runtime.driver.{ BroadcastId, ShuffleKey }
import com.asakusafw.spark.runtime.fragment.{ Fragment, GenericOutputFragment, OutputFragment }
import com.asakusafw.spark.runtime.rdd.BranchKey

import com.asakusafw.spark.extensions.iterativebatch.runtime.fixture.SparkForAll

abstract class InputSpec extends FlatSpec with SparkForAll {

  import InputSpec._

  def prepareRound(parent: String, ints: Seq[Int], round: Int): Unit = {
    val job = JobCompatibility.newJob(sc.hadoopConfiguration)
    job.setOutputKeyClass(classOf[NullWritable])
    job.setOutputValueClass(classOf[Foo])
    job.setOutputFormatClass(classOf[TemporaryOutputFormat[Foo]])

    TemporaryOutputFormat.setOutputPath(job, new Path(parent, s"foos_${round}"))

    val foos = sc.parallelize(ints).map(Foo.intToFoo(round))
    foos.saveAsNewAPIHadoopDataset(job.getConfiguration)
  }
}

@RunWith(classOf[JUnitRunner])
class TemporaryInputSpecTest extends TemporaryInputSpec

class TemporaryInputSpec extends InputSpec with RoundContextSugar with TempDirForEach {

  import InputSpec._

  behavior of classOf[TemporaryInput[_]].getSimpleName

  it should "input" in { implicit sc =>
    val tmpDir = createTempDirectoryForEach("test-").toFile.getAbsolutePath
    val path = new File(tmpDir, "foos_${round}").getAbsolutePath

    val input = new Temporary.Input(path)("input")

    for {
      round <- 0 to 1
    } {
      prepareRound(tmpDir, 0 until 100, round)

      val rc = newRoundContext(batchArguments = Map("round" -> round.toString))

      val result = Await.result(
        input.getOrCompute(rc).apply(Input).map {
          _.map {
            case (_, foo: Foo) => foo.id.get
          }.collect.toSeq
        }, Duration.Inf)

      assert(result === (0 until 100).map(i => 100 * round + i))
    }
  }

  it should "input from multiple paths" in { implicit sc =>
    val tmpDir = createTempDirectoryForEach("test-").toFile.getAbsolutePath
    val tmpDir1 = new File(tmpDir, "path1").getAbsolutePath
    val tmpDir2 = new File(tmpDir, "path2").getAbsolutePath
    val path1 = new File(tmpDir1, "foos_${round}").getAbsolutePath
    val path2 = new File(tmpDir2, "foos_${round}").getAbsolutePath

    val input = new Temporary.Input(Set(path1, path2))("input")

    for {
      round <- 0 to 1
    } {
      prepareRound(tmpDir1, 0 until 50, round)
      prepareRound(tmpDir2, 50 until 100, round)

      val rc = newRoundContext(batchArguments = Map("round" -> round.toString))

      val result = Await.result(
        input.getOrCompute(rc).apply(Input).map {
          _.map {
            case (_, foo: Foo) => foo.id.get
          }.collect.toSeq
        }, Duration.Inf)

      assert(result === (0 until 100).map(i => 100 * round + i))
    }
  }
}

@RunWith(classOf[JUnitRunner])
class DirectInputSpecTest extends DirectInputSpec

class DirectInputSpec extends InputSpec with RoundContextSugar with TempDirForEach {

  import InputSpec._

  behavior of classOf[DirectInput[_, _, _]].getSimpleName

  it should "input" in { implicit sc =>
    val tmpDir = createTempDirectoryForEach("test-").toFile.getAbsolutePath
    val path = new File(tmpDir, /*"foo_${round}"*/ "foos_0").getAbsolutePath

    val input = new Direct.Input(mkExtraConfigurations(path))("input")

    for {
      round <- 0 to 0 // 1
    } {
      prepareRound(tmpDir, 0 until 100, round)

      val rc = newRoundContext(batchArguments = Map("round" -> round.toString))

      val result = Await.result(
        input.getOrCompute(rc).apply(Input).map {
          _.map {
            case (_, foo: Foo) => foo.id.get
          }.collect.toSeq
        }, Duration.Inf)

      assert(result === (0 until 100).map(i => 100 * round + i))
    }
  }

  it should "input from multiple paths" in { implicit sc =>
    val tmpDir = createTempDirectoryForEach("test-").toFile.getAbsolutePath
    val tmpDir1 = new File(tmpDir, "path1").getAbsolutePath
    val tmpDir2 = new File(tmpDir, "path2").getAbsolutePath
    val path1 = new File(tmpDir1, /*"foos_${round}"*/ "foos_0").getAbsolutePath
    val path2 = new File(tmpDir2, /*"foos_${round}"*/ "foos_0").getAbsolutePath

    val input = new Direct.Input(mkExtraConfigurations(Set(path1, path2)))("input")

    for {
      round <- 0 to 0 // 1
    } {
      prepareRound(tmpDir1, 0 until 50, round)
      prepareRound(tmpDir2, 50 until 100, round)

      val rc = newRoundContext(batchArguments = Map("round" -> round.toString))

      val result = Await.result(
        input.getOrCompute(rc).apply(Input).map {
          _.map {
            case (_, foo: Foo) => foo.id.get
          }.collect.toSeq
        }, Duration.Inf)

      assert(result === (0 until 100).map(i => 100 * round + i))
    }
  }

  private def mkExtraConfigurations(basePath: String): Map[String, String] = {
    mkExtraConfigurations(Set(basePath))
  }

  private def mkExtraConfigurations(basePaths: Set[String]): Map[String, String] = {
    val job = JobCompatibility.newJob(new Configuration(false))
    FileInputFormat.setInputPaths(job, basePaths.map(path => new Path(path + "/part-*")).toSeq: _*)
    Map(FileInputFormat.INPUT_DIR -> job.getConfiguration.get(FileInputFormat.INPUT_DIR))
  }
}

object InputSpec {

  val Input = BranchKey(0)

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

    def intToFoo(round: Int): Int => (NullWritable, Foo) = {

      lazy val foo = new Foo()

      { i =>
        foo.id.modify(100 * round + i)
        (NullWritable.get, foo)
      }
    }
  }

  object Temporary {

    class Input(
      basePaths: Set[String])(
        val label: String)(
          implicit sc: SparkContext)
      extends TemporaryInput[Foo]()(Map.empty) {

      def this(path: String)(label: String)(implicit sc: SparkContext) = this(Set(path))(label)

      override def paths: Set[String] = basePaths.map(_ + "/part-*")

      override def branchKeys: Set[BranchKey] = Set(Input)

      override def partitioners: Map[BranchKey, Option[Partitioner]] = Map.empty

      override def orderings: Map[BranchKey, Ordering[ShuffleKey]] = Map.empty

      override def aggregations: Map[BranchKey, Aggregation[ShuffleKey, _, _]] = Map.empty

      override def shuffleKey(branch: BranchKey, value: Any): ShuffleKey = null

      override def serialize(branch: BranchKey, value: Any): Array[Byte] = {
        ???
      }

      override def deserialize(branch: BranchKey, value: Array[Byte]): Any = {
        ???
      }

      override def fragments(
        broadcasts: Map[BroadcastId, Broadcasted[_]])(
          fragmentBufferSize: Int): (Fragment[Foo], Map[BranchKey, OutputFragment[_]]) = {
        val fragment = new GenericOutputFragment[Foo](fragmentBufferSize)
        val outputs = Map(Input -> fragment)
        (fragment, outputs)
      }
    }
  }

  object Direct {

    class Input(
      val extraConfigurations: Map[String, String])(
        val label: String)(
          implicit sc: SparkContext)
      extends DirectInput[NullWritable, Foo, TemporaryInputFormat[Foo]]()(Map.empty) {

      override def branchKeys: Set[BranchKey] = Set(Input)

      override def partitioners: Map[BranchKey, Option[Partitioner]] = Map.empty

      override def orderings: Map[BranchKey, Ordering[ShuffleKey]] = Map.empty

      override def aggregations: Map[BranchKey, Aggregation[ShuffleKey, _, _]] = Map.empty

      override def shuffleKey(branch: BranchKey, value: Any): ShuffleKey = null

      override def serialize(branch: BranchKey, value: Any): Array[Byte] = {
        ???
      }

      override def deserialize(branch: BranchKey, value: Array[Byte]): Any = {
        ???
      }

      override def fragments(
        broadcasts: Map[BroadcastId, Broadcasted[_]])(
          fragmentBufferSize: Int): (Fragment[Foo], Map[BranchKey, OutputFragment[_]]) = {
        val fragment = new GenericOutputFragment[Foo](fragmentBufferSize)
        val outputs = Map(Input -> fragment)
        (fragment, outputs)
      }
    }
  }
}
