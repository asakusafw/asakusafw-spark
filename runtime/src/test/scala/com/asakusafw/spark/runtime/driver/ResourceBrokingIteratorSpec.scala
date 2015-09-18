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
import com.asakusafw.spark.runtime.rdd.BranchKey

@RunWith(classOf[JUnitRunner])
class ResourceBrokingIteratorSpecTest extends ResourceBrokingIteratorSpec

class ResourceBrokingIteratorSpec extends FlatSpec with SparkSugar {

  import ResourceBrokingIteratorSpec._

  behavior of classOf[ResourceBrokingIterator[_]].getSimpleName

  it should "broke resources" in {
    val hoges = sc.parallelize(0 until 10).map { i =>
      val hoge = new Hoge()
      hoge.id.modify(i)
      ((), hoge)
    }.asInstanceOf[RDD[(_, Hoge)]]

    val driver = new TestDriver(sc, hadoopConf, Future.successful(hoges))

    val outputs = driver.execute()

    val result = Await.result(
      outputs(Result).map {
        _.map(_._2.asInstanceOf[Hoge])
          .map(hoge => (hoge.id.get, hoge.str.getAsString))
      }, Duration.Inf).collect.toSeq
    assert(result.size === 10)
    assert(result.map(_._1) === (0 until 10))
    assert(result.map(_._2) === (0 until 10).map(i => s"test_${i}"))
  }
}

object ResourceBrokingIteratorSpec {

  val Result = BranchKey(0)

  class TestDriver(
    @transient sc: SparkContext,
    @transient hadoopConf: Broadcast[Configuration],
    @transient prev: Future[RDD[(_, Hoge)]])
    extends ExtractDriver[Hoge](sc, hadoopConf)(Seq(prev))(Map.empty) {

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

    override def fragments(broadcasts: Map[BroadcastId, Broadcast[_]]): (Fragment[Hoge], Map[BranchKey, OutputFragment[_]]) = {
      val outputs = Map(
        Result -> new HogeOutputFragment)
      val fragment = new TestFragment(outputs(Result))
      (fragment, outputs)
    }
  }

  class Hoge extends DataModel[Hoge] with Writable {

    val id = new IntOption()
    val str = new StringOption()

    override def reset(): Unit = {
      id.setNull()
      str.setNull()
    }
    override def copyFrom(other: Hoge): Unit = {
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

  class HogeOutputFragment extends OutputFragment[Hoge] {
    override def newDataModel: Hoge = new Hoge()
  }

  class TestFragment(output: Fragment[Hoge]) extends Fragment[Hoge] {

    override def add(hoge: Hoge): Unit = {
      hoge.str.modify(s"${BatchContext.get("batcharg")}_${hoge.id.get}")
      output.add(hoge)
    }

    override def reset(): Unit = {
      output.reset()
    }
  }
}
