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
import com.asakusafw.spark.runtime.operator.GenericOutputFragment
import com.asakusafw.spark.runtime.rdd.BranchKey

@RunWith(classOf[JUnitRunner])
class InputOutputDriverSpecTest extends InputOutputDriverSpec

class InputOutputDriverSpec extends FlatSpec with SparkSugar with TempDir {

  import InputOutputDriverSpec._

  behavior of "Input/OutputDriver"

  it should "output and input" in {
    val tmpDir = createTempDirectory("test-").toFile
    val path = new File(tmpDir, s"output-${EXPR_EXECUTION_ID}").getAbsolutePath

    val foos = sc.parallelize(0 until 10).map(Foo.intToFoo).asInstanceOf[RDD[(_, Foo)]]

    val terminators = mutable.Set.empty[Future[Unit]]
    new TestOutputDriver(sc, hadoopConf, Future.successful(foos), terminators, path).execute()
    Await.result(Future.sequence(terminators), Duration.Inf)

    val inputs = new TestInputDriver(sc, hadoopConf, path).execute()
    val result = Await.result(
      inputs(Result).map {
        _.map {
          case (_, foo: Foo) => foo.id.get
        }.collect.toSeq.sorted
      }, Duration.Inf)

    assert(result === (0 until 10))
  }
}

object InputOutputDriverSpec {

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

    def intToFoo = new Function1[Int, (_, Foo)] with Serializable {

      @transient var f: Foo = _
      def foo: Foo = {
        if (f == null) {
          f = new Foo()
        }
        f
      }
      override def apply(i: Int): (_, Foo) = {
        foo.id.modify(i)
        (null, foo)
      }
    }
  }

  val Result = BranchKey(0)

  class TestOutputDriver(
    @transient sc: SparkContext,
    @transient hadoopConf: Broadcast[Configuration],
    @transient input: Future[RDD[(_, Foo)]],
    @transient terminators: mutable.Set[Future[Unit]],
    val path: String)
    extends OutputDriver[Foo](sc, hadoopConf)(Seq(input), terminators) {

    override def label = "TestOutput"
  }

  class TestInputDriver(
    @transient sc: SparkContext,
    @transient hadoopConf: Broadcast[Configuration],
    basePath: String)
    extends InputDriver[NullWritable, Foo, TemporaryInputFormat[Foo]](sc, hadoopConf)(Map.empty) {

    override def label = "TestInput"

    override def paths: Option[Set[String]] = Option(Set(basePath + "/part-*"))

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

    override def fragments(broadcasts: Map[BroadcastId, Broadcast[_]]): (Fragment[Foo], Map[BranchKey, OutputFragment[_]]) = {
      val fragment = new GenericOutputFragment[Foo]()
      val outputs = Map(Result -> fragment)
      (fragment, outputs)
    }
  }

}
