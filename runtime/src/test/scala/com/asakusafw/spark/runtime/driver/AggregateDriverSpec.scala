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
import java.util.Arrays

import scala.concurrent.{ Await, Future }
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.Writable
import org.apache.spark._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

import com.asakusafw.runtime.model.DataModel
import com.asakusafw.runtime.value.IntOption
import com.asakusafw.spark.runtime.aggregation.Aggregation
import com.asakusafw.spark.runtime.fragment._
import com.asakusafw.spark.runtime.io.WritableSerDe
import com.asakusafw.spark.runtime.rdd.BranchKey

@RunWith(classOf[JUnitRunner])
class AggregateDriverSpecTest extends AggregateDriverSpec

class AggregateDriverSpec extends FlatSpec with SparkSugar {

  import AggregateDriverSpec._

  behavior of classOf[AggregateDriver[_, _]].getSimpleName

  it should "aggregate with map-side combine" in {
    import TotalAggregate._
    val f = new Function1[Int, (ShuffleKey, Hoge)] with Serializable {

      @transient var h: Hoge = _

      def hoge: Hoge = {
        if (h == null) {
          h = new Hoge()
        }
        h
      }

      override def apply(i: Int): (ShuffleKey, Hoge) = {
        hoge.id.modify(i % 2)
        hoge.price.modify(i * 100)
        val shuffleKey = new ShuffleKey(
          WritableSerDe.serialize(hoge.id), WritableSerDe.serialize(hoge.price))
        (shuffleKey, hoge)
      }
    }
    val hoges = sc.parallelize(0 until 10).map(f)

    val part = new HashPartitioner(2)
    val aggregation = new TestAggregation(true)

    val driver = new TestAggregateDriver(
      sc, hadoopConf, Future.successful(hoges),
      Option(new SortOrdering()),
      part,
      aggregation)

    val outputs = driver.execute()
    assert(Await.result(
      outputs(Result).map {
        _.map {
          case (_, hoge: Hoge) => (hoge.id.get, hoge.price.get)
        }
      }, Duration.Inf).collect.toSeq.sortBy(_._1) ===
      Seq((0, (0 until 10 by 2).map(_ * 100).sum), (1, (1 until 10 by 2).map(_ * 100).sum)))
  }

  it should "aggregate without map-side combine" in {
    import TotalAggregate._
    val f = new Function1[Int, (ShuffleKey, Hoge)] with Serializable {

      @transient var h: Hoge = _

      def hoge: Hoge = {
        if (h == null) {
          h = new Hoge()
        }
        h
      }

      override def apply(i: Int): (ShuffleKey, Hoge) = {
        hoge.id.modify(i % 2)
        hoge.price.modify(i * 100)
        val shuffleKey = new ShuffleKey(
          WritableSerDe.serialize(hoge.id),
          WritableSerDe.serialize(hoge.price))
        (shuffleKey, hoge)
      }
    }
    val hoges = sc.parallelize(0 until 10).map(f)

    val part = new HashPartitioner(2)
    val aggregation = new TestAggregation(false)

    val driver = new TestAggregateDriver(
      sc, hadoopConf, Future.successful(hoges),
      Option(new SortOrdering()),
      part,
      aggregation)

    val outputs = driver.execute()
    assert(Await.result(
      outputs(Result).map {
        _.map {
          case (_, hoge: Hoge) => (hoge.id.get, hoge.price.get)
        }
      }, Duration.Inf).collect.toSeq.sortBy(_._1) ===
      Seq((0, (0 until 10 by 2).map(_ * 100).sum), (1, (1 until 10 by 2).map(_ * 100).sum)))
  }

  it should "aggregate partially" in {
    import PartialAggregate._
    val f = new Function1[Int, (ShuffleKey, Hoge)] with Serializable {

      @transient var h: Hoge = _

      def hoge: Hoge = {
        if (h == null) {
          h = new Hoge()
        }
        h
      }

      override def apply(i: Int): (ShuffleKey, Hoge) = {
        hoge.id.modify(i % 2)
        hoge.price.modify(i * 100)
        val shuffleKey = new ShuffleKey(
          WritableSerDe.serialize(hoge.id),
          WritableSerDe.serialize(hoge.price))
        (shuffleKey, hoge)
      }
    }
    val hoges = sc.parallelize(0 until 10, 2).map(f).asInstanceOf[RDD[(_, Hoge)]]

    val driver = new TestPartialAggregationExtractDriver(sc, hadoopConf, Future.successful(hoges))

    val outputs = driver.execute()
    assert(Await.result(
      outputs(Result1).map {
        _.map {
          case (_, hoge: Hoge) => (hoge.id.get, hoge.price.get)
        }
      }, Duration.Inf).collect.toSeq.sortBy(_._1) ===
      Seq(
        (0, (0 until 10).filter(_ % 2 == 0).filter(_ < 5).map(_ * 100).sum),
        (0, (0 until 10).filter(_ % 2 == 0).filterNot(_ < 5).map(_ * 100).sum),
        (1, (0 until 10).filter(_ % 2 == 1).filter(_ < 5).map(_ * 100).sum),
        (1, (0 until 10).filter(_ % 2 == 1).filterNot(_ < 5).map(_ * 100).sum)))
    assert(Await.result(
      outputs(Result2).map {
        _.map {
          case (key, hoge: Hoge) => hoge.price.get
        }
      }, Duration.Inf).collect.toSeq === (0 until 10).map(100 * _))
  }
}

object AggregateDriverSpec {

  class Hoge extends DataModel[Hoge] with Writable {

    val id = new IntOption()
    val price = new IntOption()

    override def reset(): Unit = {
      id.setNull()
      price.setNull()
    }
    override def copyFrom(other: Hoge): Unit = {
      id.copyFrom(other.id)
      price.copyFrom(other.price)
    }
    override def readFields(in: DataInput): Unit = {
      id.readFields(in)
      price.readFields(in)
    }
    override def write(out: DataOutput): Unit = {
      id.write(out)
      price.write(out)
    }
  }

  class HogeOutputFragment extends OutputFragment[Hoge] {
    override def newDataModel: Hoge = new Hoge()
  }

  class TestAggregation(val mapSideCombine: Boolean)
    extends Aggregation[ShuffleKey, Hoge, Hoge] {

    override def newCombiner(): Hoge = {
      new Hoge()
    }

    override def initCombinerByValue(combiner: Hoge, value: Hoge): Hoge = {
      combiner.copyFrom(value)
      combiner
    }

    override def mergeValue(combiner: Hoge, value: Hoge): Hoge = {
      combiner.price.add(value.price)
      combiner
    }

    override def initCombinerByCombiner(comb1: Hoge, comb2: Hoge): Hoge = {
      comb1.copyFrom(comb2)
      comb1
    }

    override def mergeCombiners(comb1: Hoge, comb2: Hoge): Hoge = {
      comb1.price.add(comb2.price)
      comb1
    }
  }

  object TotalAggregate {
    val Result = BranchKey(0)

    class TestAggregateDriver(
      @transient sc: SparkContext,
      @transient hadoopConf: Broadcast[Configuration],
      @transient prev: Future[RDD[(ShuffleKey, Hoge)]],
      @transient sort: Option[Ordering[ShuffleKey]],
      @transient part: Partitioner,
      val aggregation: Aggregation[ShuffleKey, Hoge, Hoge])
      extends AggregateDriver[Hoge, Hoge](sc, hadoopConf, Map.empty, Seq(prev), sort, part) {

      override def label = "TestAggregation"

      override def branchKeys: Set[BranchKey] = Set(Result)

      override def partitioners: Map[BranchKey, Option[Partitioner]] = Map.empty

      override def orderings: Map[BranchKey, Ordering[ShuffleKey]] = Map.empty

      override def aggregations: Map[BranchKey, Aggregation[ShuffleKey, _, _]] = Map.empty

      override def shuffleKey(branch: BranchKey, value: Any): ShuffleKey = {
        new ShuffleKey(WritableSerDe.serialize(value.asInstanceOf[Hoge].id), Array.empty)
      }

      override def serialize(branch: BranchKey, value: Any): Array[Byte] = {
        ???
      }

      override def deserialize(branch: BranchKey, value: Array[Byte]): Any = {
        ???
      }

      override def fragments(broadcasts: Map[BroadcastId, Broadcast[_]]): (Fragment[Hoge], Map[BranchKey, OutputFragment[_]]) = {
        val fragment = new HogeOutputFragment
        val outputs = Map(Result -> fragment)
        (fragment, outputs)
      }
    }

    class SortOrdering extends Ordering[ShuffleKey] {

      override def compare(x: ShuffleKey, y: ShuffleKey): Int = {
        val xGrouping = x.grouping
        val yGrouping = y.grouping
        val cmp = IntOption.compareBytes(xGrouping, 0, xGrouping.length, yGrouping, 0, yGrouping.length)
        if (cmp == 0) {
          val xOrdering = x.ordering
          val yOrdering = y.ordering
          IntOption.compareBytes(yOrdering, 0, yOrdering.length, xOrdering, 0, xOrdering.length)
        } else {
          cmp
        }
      }
    }
  }

  object PartialAggregate {
    val Result1 = BranchKey(0)
    val Result2 = BranchKey(1)

    class TestPartialAggregationExtractDriver(
      @transient sc: SparkContext,
      @transient hadoopConf: Broadcast[Configuration],
      @transient prev: Future[RDD[(_, Hoge)]])
      extends ExtractDriver[Hoge](sc, hadoopConf, Map.empty, Seq(prev)) {

      override def label = "TestPartialAggregation"

      override def branchKeys: Set[BranchKey] = Set(Result1, Result2)

      override def partitioners: Map[BranchKey, Option[Partitioner]] = {
        Map(Result1 -> Some(new HashPartitioner(2)))
      }

      override def orderings: Map[BranchKey, Ordering[ShuffleKey]] = Map.empty

      override def aggregations: Map[BranchKey, Aggregation[ShuffleKey, _, _]] = {
        Map(Result1 -> new TestAggregation(true))
      }

      override def shuffleKey(branch: BranchKey, value: Any): ShuffleKey = {
        new ShuffleKey(WritableSerDe.serialize(value.asInstanceOf[Hoge].id), Array.empty)
      }

      override def serialize(branch: BranchKey, value: Any): Array[Byte] = {
        WritableSerDe.serialize(value.asInstanceOf[Writable])
      }

      @transient var h: Hoge = _

      def hoge = {
        if (h == null) {
          h = new Hoge()
        }
        h
      }

      override def deserialize(branch: BranchKey, value: Array[Byte]): Any = {
        WritableSerDe.deserialize(value, hoge)
        hoge
      }

      override def fragments(broadcasts: Map[BroadcastId, Broadcast[_]]): (Fragment[Hoge], Map[BranchKey, OutputFragment[_]]) = {
        val fragment1 = new HogeOutputFragment
        val fragment2 = new HogeOutputFragment
        (new EdgeFragment[Hoge](Array(fragment1, fragment2)) {
          override def newDataModel(): Hoge = new Hoge()
        }, Map(
          Result1 -> fragment1,
          Result2 -> fragment2))
      }
    }
  }
}
