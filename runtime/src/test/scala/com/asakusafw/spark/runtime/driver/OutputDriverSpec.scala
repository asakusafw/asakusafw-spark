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

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.concurrent.{ Await, Future }
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{ NullWritable, Writable }
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

import com.asakusafw.bridge.stage.StageInfo
import com.asakusafw.runtime.compatibility.JobCompatibility
import com.asakusafw.runtime.model.DataModel
import com.asakusafw.runtime.stage.StageConstants.EXPR_EXECUTION_ID
import com.asakusafw.runtime.stage.input.TemporaryInputFormat
import com.asakusafw.runtime.value.IntOption
import com.asakusafw.spark.runtime.rdd.BranchKey

@RunWith(classOf[JUnitRunner])
class OutputDriverSpecTest extends OutputDriverSpec

class OutputDriverSpec extends FlatSpec with SparkForAll with HadoopConfForEach with TempDirForEach {

  import OutputDriverSpec._

  behavior of classOf[OutputDriver[_]].getSimpleName

  def readResult(path: String, hadoopConf: Broadcast[Configuration]): Seq[Int] = {
    val job = JobCompatibility.newJob(hadoopConf.value)

    val stageInfo = StageInfo.deserialize(job.getConfiguration.get(StageInfo.KEY_NAME))
    FileInputFormat.setInputPaths(job, new Path(stageInfo.resolveUserVariables(path + "/part-*")))

    sc.newAPIHadoopRDD(
      job.getConfiguration,
      classOf[TemporaryInputFormat[Foo]],
      classOf[NullWritable],
      classOf[Foo]).map(_._2.id.get).collect.toSeq.sorted
  }

  for {
    numSlices <- Seq(8, 4)
  } {
    val conf = s"[numSlices = ${numSlices}]"

    it should s"output: ${conf}" in {
      val tmpDir = createTempDirectoryForEach("test-").toFile
      val path = new File(tmpDir, s"output-${EXPR_EXECUTION_ID}").getAbsolutePath

      val foos = sc.parallelize(0 until 10, numSlices).map(Foo.intToFoo).asInstanceOf[RDD[(_, Foo)]]

      val terminators = mutable.Set.empty[Future[Unit]]
      new TestOutputDriver(sc, hadoopConf, Future.successful(foos), terminators, path).execute()
      Await.result(Future.sequence(terminators), Duration.Inf)

      val result = readResult(path, hadoopConf)
      assert(result === (0 until 10))
    }

    it should s"output multiple prevs: ${conf}" in {
      val tmpDir = createTempDirectoryForEach("test-").toFile
      val path = new File(tmpDir, s"output-${EXPR_EXECUTION_ID}").getAbsolutePath

      val foos1 = sc.parallelize(0 until 5, numSlices).map(Foo.intToFoo).asInstanceOf[RDD[(_, Foo)]]
      val foos2 = sc.parallelize(5 until 10, numSlices).map(Foo.intToFoo).asInstanceOf[RDD[(_, Foo)]]

      val terminators = mutable.Set.empty[Future[Unit]]
      new TestOutputDriver(
        sc,
        hadoopConf,
        Seq(Future.successful(foos1), Future.successful(foos2)),
        terminators,
        path).execute()
      Await.result(Future.sequence(terminators), Duration.Inf)

      val result = readResult(path, hadoopConf)
      assert(result === (0 until 10))
    }
  }
}

@RunWith(classOf[JUnitRunner])
class OutputDriverWithParallelismSpecTest extends OutputDriverWithParallelismSpec

class OutputDriverWithParallelismSpec extends OutputDriverSpec {

  override def configure(conf: SparkConf): SparkConf = {
    conf.set("spark.default.parallelism", 8.toString)
  }
}

object OutputDriverSpec {

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
        (null, foo)
      }
    }
  }

  val Result = BranchKey(0)

  class TestOutputDriver(
    @transient sc: SparkContext,
    @transient hadoopConf: Broadcast[Configuration],
    @transient inputs: Seq[Future[RDD[(_, Foo)]]],
    @transient terminators: mutable.Set[Future[Unit]],
    val path: String)
    extends OutputDriver[Foo](sc, hadoopConf)(inputs, terminators) {

    def this(
      sc: SparkContext,
      hadoopConf: Broadcast[Configuration],
      input: Future[RDD[(_, Foo)]],
      terminators: mutable.Set[Future[Unit]],
      path: String) = this(sc, hadoopConf, Seq(input), terminators, path)

    override def label = "TestOutput"
  }
}
