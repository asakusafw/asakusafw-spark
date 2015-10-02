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

import java.io.{ DataInput, DataOutput }

import scala.concurrent.{ Await, Future }
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.Writable
import org.apache.spark._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

import com.asakusafw.bridge.api.BatchContext
import com.asakusafw.runtime.model.DataModel
import com.asakusafw.runtime.value.{ IntOption, StringOption }
import com.asakusafw.spark.runtime.aggregation.Aggregation
import com.asakusafw.spark.runtime.fragment._
import com.asakusafw.spark.runtime.operator.GenericOutputFragment
import com.asakusafw.spark.runtime.rdd.BranchKey

@RunWith(classOf[JUnitRunner])
class ResourceBrokingIteratorSpecTest extends ResourceBrokingIteratorSpec

class ResourceBrokingIteratorSpec extends FlatSpec with SparkSugar {

  import ResourceBrokingIteratorSpec._

  behavior of classOf[ResourceBrokingIterator[_]].getSimpleName

  it should "broke resources" in {
    val foos = sc.parallelize(0 until 10).map { i =>
      val foo = new Foo()
      foo.id.modify(i)
      ((), foo)
    }.asInstanceOf[RDD[(_, Foo)]]

    val driver = new TestDriver(sc, hadoopConf, Future.successful(foos))

    val outputs = driver.execute()

    val result = Await.result(
      outputs(Result).map {
        _.map {
          case (_, foo: Foo) => (foo.id.get, foo.str.getAsString)
        }.collect.toSeq
      }, Duration.Inf)
    assert(result.size === 10)
    assert(result.map(_._1) === (0 until 10))
    assert(result.map(_._2) === (0 until 10).map(i => s"test_${i}"))
  }
}

object ResourceBrokingIteratorSpec {

  class Foo extends DataModel[Foo] with Writable {

    val id = new IntOption()
    val str = new StringOption()

    override def reset(): Unit = {
      id.setNull()
      str.setNull()
    }
    override def copyFrom(other: Foo): Unit = {
      id.copyFrom(other.id)
      str.copyFrom(other.str)
    }
    override def readFields(in: DataInput): Unit = {
      id.readFields(in)
      str.readFields(in)
    }
    override def write(out: DataOutput): Unit = {
      id.write(out)
      str.write(out)
    }
  }

  val Result = BranchKey(0)

  class TestDriver(
    @transient sc: SparkContext,
    @transient hadoopConf: Broadcast[Configuration],
    @transient prev: Future[RDD[(_, Foo)]])
    extends ExtractDriver[Foo](sc, hadoopConf)(Seq(prev))(Map.empty) {

    override def label = "TestMap"

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
      val outputs = Map(
        Result -> new GenericOutputFragment[Foo]())
      val fragment = new TestFragment(outputs(Result))
      (fragment, outputs)
    }
  }

  class TestFragment(output: Fragment[Foo]) extends Fragment[Foo] {

    override def add(foo: Foo): Unit = {
      foo.str.modify(s"${BatchContext.get("batcharg")}_${foo.id.get}")
      output.add(foo)
    }

    override def reset(): Unit = {
      output.reset()
    }
  }
}
