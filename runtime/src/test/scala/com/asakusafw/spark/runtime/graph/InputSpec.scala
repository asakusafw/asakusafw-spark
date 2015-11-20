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

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{ NullWritable, Writable }
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.spark.{ Partitioner, SparkConf, SparkContext }
import org.apache.spark.broadcast.{ Broadcast => Broadcasted }

import com.asakusafw.runtime.compatibility.JobCompatibility
import com.asakusafw.runtime.model.DataModel
import com.asakusafw.runtime.stage.input.TemporaryInputFormat
import com.asakusafw.runtime.stage.output.TemporaryOutputFormat
import com.asakusafw.runtime.value.IntOption
import com.asakusafw.spark.runtime.TempDirForEach
import com.asakusafw.spark.runtime.aggregation.Aggregation
import com.asakusafw.spark.runtime.driver.{ BroadcastId, ShuffleKey }
import com.asakusafw.spark.runtime.fixture.SparkForAll
import com.asakusafw.spark.runtime.fragment.{ Fragment, GenericOutputFragment, OutputFragment }
import com.asakusafw.spark.runtime.rdd.BranchKey

abstract class InputSpec extends FlatSpec with SparkForAll {

  import InputSpec._

  def prepareRound(path: String, ints: Seq[Int]): Unit = {
    val job = JobCompatibility.newJob(sc.hadoopConfiguration)
    job.setOutputKeyClass(classOf[NullWritable])
    job.setOutputValueClass(classOf[Foo])
    job.setOutputFormatClass(classOf[TemporaryOutputFormat[Foo]])

    TemporaryOutputFormat.setOutputPath(job, new Path(path))

    val foos = sc.parallelize(ints).map(Foo.intToFoo)
    foos.saveAsNewAPIHadoopDataset(job.getConfiguration)
  }
}

@RunWith(classOf[JUnitRunner])
class TemporaryInputSpecTest extends TemporaryInputSpec

class TemporaryInputSpec extends InputSpec with RoundContextSugar with TempDirForEach {

  import InputSpec._

  behavior of classOf[TemporaryInput[_]].getSimpleName

  for {
    numSlices <- Seq(None, Some(8), Some(4))
  } {
    val conf = s"numSlices = ${numSlices}"

    it should s"input: [${conf}]" in { implicit sc =>
      val tmpDir = createTempDirectoryForEach("test-").toFile.getAbsolutePath
      val path = new File(tmpDir, "foos").getAbsolutePath

      val input = new Temporary.Input(path)("input")

      prepareRound(path, 0 until 100)

      val rc = newRoundContext()

      val result = Await.result(
        input.getOrCompute(rc).apply(Input).map {
          _.map {
            case (_, foo: Foo) => foo.id.get
          }.collect.toSeq.sorted
        }, Duration.Inf)

      assert(result === (0 until 100))
    }

    it should s"input from multiple paths: [${conf}]" in { implicit sc =>
      val tmpDir = createTempDirectoryForEach("test-").toFile.getAbsolutePath
      val path1 = new File(tmpDir, "foos1").getAbsolutePath
      val path2 = new File(tmpDir, "foos2").getAbsolutePath

      val input = new Temporary.Input(Set(path1, path2))("input")

      prepareRound(path1, 0 until 50)
      prepareRound(path2, 50 until 100)

      val rc = newRoundContext()

      val result = Await.result(
        input.getOrCompute(rc).apply(Input).map {
          _.map {
            case (_, foo: Foo) => foo.id.get
          }.collect.toSeq.sorted
        }, Duration.Inf)

      assert(result === (0 until 100))
    }
  }
}

@RunWith(classOf[JUnitRunner])
class TemporaryInputWithParallelismSpecTest extends TemporaryInputWithParallelismSpec

class TemporaryInputWithParallelismSpec extends TemporaryInputSpec {

  override def configure(conf: SparkConf): SparkConf = {
    conf.set("spark.default.parallelism", 8.toString)
  }
}

@RunWith(classOf[JUnitRunner])
class DirectInputSpecTest extends DirectInputSpec

class DirectInputSpec extends InputSpec with RoundContextSugar with TempDirForEach {

  import InputSpec._

  behavior of classOf[DirectInput[_, _, _]].getSimpleName

  for {
    numSlices <- Seq(None, Some(8), Some(4))
  } {
    val conf = s"numSlices = ${numSlices}"

    it should s"input: [${conf}]" in { implicit sc =>
      val tmpDir = createTempDirectoryForEach("test-").toFile.getAbsolutePath
      val path = new File(tmpDir, "foos").getAbsolutePath

      val input = new Direct.Input(mkExtraConfigurations(path))("input")

      prepareRound(path, 0 until 100)

      val rc = newRoundContext()

      val result = Await.result(
        input.getOrCompute(rc).apply(Input).map {
          _.map {
            case (_, foo: Foo) => foo.id.get
          }.collect.toSeq.sorted
        }, Duration.Inf)

      assert(result === (0 until 100))
    }

    it should s"input from multiple paths: [${conf}]" in { implicit sc =>
      val tmpDir = createTempDirectoryForEach("test-").toFile.getAbsolutePath
      val path1 = new File(tmpDir, "foos1").getAbsolutePath
      val path2 = new File(tmpDir, "foos2").getAbsolutePath

      val input = new Direct.Input(mkExtraConfigurations(Set(path1, path2)))("input")

      prepareRound(path1, 0 until 50)
      prepareRound(path2, 50 until 100)

      val rc = newRoundContext()

      val result = Await.result(
        input.getOrCompute(rc).apply(Input).map {
          _.map {
            case (_, foo: Foo) => foo.id.get
          }.collect.toSeq.sorted
        }, Duration.Inf)

      assert(result === (0 until 100))
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

@RunWith(classOf[JUnitRunner])
class DirectInputWithParallelismSpecTest extends DirectInputWithParallelismSpec

class DirectInputWithParallelismSpec extends DirectInputSpec {

  override def configure(conf: SparkConf): SparkConf = {
    conf.set("spark.default.parallelism", 8.toString)
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

    def intToFoo: Int => (NullWritable, Foo) = {

      lazy val foo = new Foo()

      { i =>
        foo.id.modify(i)
        (NullWritable.get, foo)
      }
    }
  }

  object Temporary {

    class Input(
      basePaths: Set[String])(
        val label: String)(
          implicit sc: SparkContext)
      extends TemporaryInput[Foo](Map.empty)
      with ComputeOnce {

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
      extends DirectInput[TemporaryInputFormat[Foo], NullWritable, Foo](Map.empty)
      with ComputeOnce {

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
