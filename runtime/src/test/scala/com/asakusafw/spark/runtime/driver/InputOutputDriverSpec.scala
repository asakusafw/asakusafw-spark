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
import java.nio.file.{ Files, Path }

import scala.collection.mutable
import scala.collection.JavaConversions._
import scala.concurrent.{ Await, Future }
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.reflect.ClassTag

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.{ NullWritable, Writable }
import org.apache.spark._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd._

import com.asakusafw.runtime.model.DataModel
import com.asakusafw.runtime.stage.StageConstants.EXPR_EXECUTION_ID
import com.asakusafw.runtime.stage.input.TemporaryInputFormat
import com.asakusafw.runtime.value.IntOption
import com.asakusafw.spark.runtime.aggregation.Aggregation
import com.asakusafw.spark.runtime.fragment._
import com.asakusafw.spark.runtime.rdd.BranchKey

@RunWith(classOf[JUnitRunner])
class InputOutputDriverSpecTest extends InputOutputDriverSpec

class InputOutputDriverSpec extends FlatSpec with SparkSugar {

  import InputOutputDriverSpec._

  behavior of "Input/OutputDriver"

  it should "output and input" in {
    val tmpDir = createTempDirectory().toFile
    val path = new File(tmpDir, s"output-${EXPR_EXECUTION_ID}").getAbsolutePath

    val f = new Function1[Int, (_, Hoge)] with Serializable {
      @transient var h: Hoge = _
      def hoge: Hoge = {
        if (h == null) {
          h = new Hoge()
        }
        h
      }
      override def apply(i: Int): (_, Hoge) = {
        hoge.id.modify(i)
        (null, hoge)
      }
    }

    val hoges = sc.parallelize(0 until 10).map(f).asInstanceOf[RDD[(_, Hoge)]]

    val terminators = mutable.Set.empty[Future[Unit]]
    new TestOutputDriver(sc, hadoopConf, Future.successful(hoges), terminators, path).execute()
    terminators.foreach {
      Await.ready(_, Duration.Inf)
    }

    val inputs = new TestInputDriver(sc, hadoopConf, path).execute()
    assert(Await.result(
      inputs(HogeResult).map {
        _.map(_._2.asInstanceOf[Hoge].id.get)
      }, Duration.Inf).collect.toSeq.sorted === (0 until 10))
  }

  private def createTempDirectory(): Path = {
    val tmpDir = Files.createTempDirectory(s"test-")
    sys.addShutdownHook {
      def deleteRecursively(path: Path): Unit = {
        if (Files.isDirectory(path)) {
          Files.newDirectoryStream(path).foreach(deleteRecursively)
        }
        Files.delete(path)
      }
      deleteRecursively(tmpDir)
    }
    tmpDir
  }
}

object InputOutputDriverSpec {

  val HogeResult = BranchKey(0)

  class TestOutputDriver(
    @transient sc: SparkContext,
    @transient hadoopConf: Broadcast[Configuration],
    @transient input: Future[RDD[(_, Hoge)]],
    @transient terminators: mutable.Set[Future[Unit]],
    val path: String)
      extends OutputDriver[Hoge](sc, hadoopConf, Seq(input), terminators) {

    override def label = "TestOutput"
  }

  class TestInputDriver(
    @transient sc: SparkContext,
    @transient hadoopConf: Broadcast[Configuration],
    basePath: String)
      extends InputDriver[NullWritable, Hoge, TemporaryInputFormat[Hoge]](sc, hadoopConf, Map.empty) {

    override def label = "TestInput"

    override def paths: Option[Set[String]] = Option(Set(basePath + "/part-*"))

    override def extraConfigurations: Map[String, String] = Map.empty

    override def branchKeys: Set[BranchKey] = Set(HogeResult)

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

    override def fragments(broadcasts: Map[BroadcastId, Broadcast[_]]): (Fragment[Hoge], Map[BranchKey, OutputFragment[_]]) = {
      val fragment = new HogeOutputFragment
      val outputs = Map(HogeResult -> fragment)
      (fragment, outputs)
    }
  }

  class Hoge extends DataModel[Hoge] with Writable {

    val id = new IntOption()

    override def reset(): Unit = {
      id.setNull()
    }
    override def copyFrom(other: Hoge): Unit = {
      id.copyFrom(other.id)
    }
    override def readFields(in: DataInput): Unit = {
      id.readFields(in)
    }
    override def write(out: DataOutput): Unit = {
      id.write(out)
    }
  }

  class HogeOutputFragment extends OutputFragment[Hoge] {
    override def newDataModel: Hoge = new Hoge()
  }
}
