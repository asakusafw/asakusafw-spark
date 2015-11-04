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
package driver

import org.junit.runner.RunWith
import org.scalatest.FlatSpec
import org.scalatest.junit.JUnitRunner

import java.io.{ DataInput, DataOutput, File }
import java.util.regex.Pattern

import scala.collection.JavaConversions._
import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{ NullWritable, Writable }
import org.apache.spark.{ Partitioner, SparkConf, SparkContext }
import org.apache.spark.broadcast.Broadcast

import com.asakusafw.runtime.compatibility.JobCompatibility
import com.asakusafw.runtime.model.DataModel
import com.asakusafw.runtime.stage.StageConstants.EXPR_EXECUTION_ID
import com.asakusafw.runtime.stage.input.TemporaryInputFormat
import com.asakusafw.runtime.stage.output.TemporaryOutputFormat
import com.asakusafw.runtime.value.IntOption
import com.asakusafw.spark.runtime.aggregation.Aggregation
import com.asakusafw.spark.runtime.fragment.{ Fragment, GenericOutputFragment, OutputFragment }
import com.asakusafw.spark.runtime.rdd.BranchKey

@RunWith(classOf[JUnitRunner])
class InputDriverSpecTest extends InputDriverSpec

class InputDriverSpec extends FlatSpec with SparkForAll with HadoopConfForEach with TempDirForEach {

  import InputDriverSpec._

  behavior of classOf[InputDriver[_, _, _]].getSimpleName

  def prepareInput(path: String, ints: Seq[Int], numSlices: Int): Unit = {
    val job = JobCompatibility.newJob(sc.hadoopConfiguration)
    job.setOutputKeyClass(classOf[NullWritable])
    job.setOutputValueClass(classOf[Foo])
    job.setOutputFormatClass(classOf[TemporaryOutputFormat[Foo]])

    TemporaryOutputFormat.setOutputPath(
      job,
      new Path(path.replaceAll(Pattern.quote(EXPR_EXECUTION_ID), "executionId")))

    val foos = sc.parallelize(ints, numSlices).map(Foo.intToFoo)
    foos.saveAsNewAPIHadoopDataset(job.getConfiguration)
  }

  for {
    numSlices <- Seq(8, 4)
  } {
    val conf = s"[numSlices = ${numSlices}]"

    it should s"input: ${conf}" in {
      val tmpDir = createTempDirectoryForEach("test-").toFile
      val path = new File(tmpDir, s"output-${EXPR_EXECUTION_ID}").getAbsolutePath

      prepareInput(path, 0 until 10, numSlices)

      val inputs = new TestInputDriver(sc, hadoopConf, path).execute()
      val result = Await.result(
        inputs(Result).map {
          _.map {
            case (_, foo: Foo) => foo.id.get
          }.collect.toSeq.sorted
        }, Duration.Inf)

      assert(result === (0 until 10))
    }

    it should s"input multiple paths: ${conf}" in {
      val tmpDir = createTempDirectoryForEach("test-").toFile
      val path1 = new File(tmpDir, s"output-${EXPR_EXECUTION_ID}/foos1").getAbsolutePath
      val path2 = new File(tmpDir, s"output-${EXPR_EXECUTION_ID}/foos2").getAbsolutePath

      prepareInput(path1, 0 until 5, numSlices)
      prepareInput(path2, 5 until 10, numSlices)

      val inputs = new TestInputDriver(sc, hadoopConf, Set(path1, path2)).execute()
      val result = Await.result(
        inputs(Result).map {
          _.map {
            case (_, foo: Foo) => foo.id.get
          }.collect.toSeq.sorted
        }, Duration.Inf)

      assert(result === (0 until 10))
    }
  }
}

@RunWith(classOf[JUnitRunner])
class InputDriverWithParallelismSpecTest extends InputDriverWithParallelismSpec

class InputDriverWithParallelismSpec extends InputDriverSpec {

  override def configure(conf: SparkConf): SparkConf = {
    conf.set("spark.default.parallelism", 8.toString)
  }
}

object InputDriverSpec {

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

  val Result = BranchKey(0)

  class TestInputDriver(
    @transient sc: SparkContext,
    @transient hadoopConf: Broadcast[Configuration],
    basePaths: Set[String])
    extends InputDriver[NullWritable, Foo, TemporaryInputFormat[Foo]](sc, hadoopConf)(Map.empty) {

    def this(sc: SparkContext, hadoopConf: Broadcast[Configuration], basePath: String) =
      this(sc, hadoopConf, Set(basePath))

    override def label = "TestInput"

    override def paths: Option[Set[String]] = Option(basePaths.map(_ + "/part-*"))

    override def extraConfigurations: Map[String, String] = Map.empty

    override def branchKeys: Set[BranchKey] = Set(Result)

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
      broadcasts: Map[BroadcastId, Broadcast[_]])(
        fragmentBufferSize: Int): (Fragment[Foo], Map[BranchKey, OutputFragment[_]]) = {
      val fragment = new GenericOutputFragment[Foo](fragmentBufferSize)
      val outputs = Map(Result -> fragment)
      (fragment, outputs)
    }
  }
}
