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
import org.apache.spark.{ HashPartitioner, Partitioner, SparkContext }
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

import com.asakusafw.runtime.model.DataModel
import com.asakusafw.runtime.value.IntOption
import com.asakusafw.spark.runtime.aggregation.Aggregation
import com.asakusafw.spark.runtime.fragment.{
  Fragment,
  GenericEdgeFragment,
  GenericOutputFragment,
  OutputFragment
}
import com.asakusafw.spark.runtime.io.WritableSerDe
import com.asakusafw.spark.runtime.rdd.BranchKey

@RunWith(classOf[JUnitRunner])
class AggregateDriverSpecTest extends AggregateDriverSpec

class AggregateDriverSpec extends FlatSpec with SparkForAll with HadoopConfForEach {

  import AggregateDriverSpec._

  behavior of classOf[AggregateDriver[_, _]].getSimpleName

  for {
    mapSideCombine <- Seq(true, false)
  } {
    it should s"aggregate with map-side combine = ${mapSideCombine}" in {
      import TotalAggregate._

      val foos = sc.parallelize(0 until 10).map(Foo.intToFoo)

      val partitioner = new HashPartitioner(2)
      val aggregation = new TestAggregation(mapSideCombine)

      val driver = new TestAggregateDriver(
        sc, hadoopConf, Future.successful(foos),
        Option(new SortOrdering()),
        partitioner,
        aggregation)

      val outputs = driver.execute()
      val result = Await.result(
        outputs(Result).map {
          _.map {
            case (_, foo: Foo) => (foo.id.get, foo.sum.get)
          }.collect.toSeq.sortBy(_._1)
        }, Duration.Inf)

      assert(result ===
        Seq((0, (0 until 10 by 2).map(_ * 100).sum), (1, (1 until 10 by 2).map(_ * 100).sum)))
    }
  }

  it should "aggregate partially" in {
    import PartialAggregate._

    val foos = sc.parallelize(0 until 10, 2).map(Foo.intToFoo).asInstanceOf[RDD[(_, Foo)]]

    val driver = new TestPartialAggregationExtractDriver(sc, hadoopConf, Future.successful(foos))

    val outputs = driver.execute()
    val (result1, result2) = Await.result(
      outputs(Result1).map {
        _.map {
          case (_, foo: Foo) => (foo.id.get, foo.sum.get)
        }.collect.toSeq.sortBy(_._1)
      }.zip {
        outputs(Result2).map {
          _.map {
            case (key, foo: Foo) => foo.sum.get
          }.collect.toSeq
        }
      }, Duration.Inf)

    assert(result1 ===
      Seq(
        (0, (0 until 10).filter(_ % 2 == 0).filter(_ < 5).map(_ * 100).sum),
        (0, (0 until 10).filter(_ % 2 == 0).filterNot(_ < 5).map(_ * 100).sum),
        (1, (0 until 10).filter(_ % 2 == 1).filter(_ < 5).map(_ * 100).sum),
        (1, (0 until 10).filter(_ % 2 == 1).filterNot(_ < 5).map(_ * 100).sum)))
    assert(result2 === (0 until 10).map(100 * _))
  }
}

object AggregateDriverSpec {

  class Foo extends DataModel[Foo] with Writable {

    val id = new IntOption()
    val sum = new IntOption()

    override def reset(): Unit = {
      id.setNull()
      sum.setNull()
    }
    override def copyFrom(other: Foo): Unit = {
      id.copyFrom(other.id)
      sum.copyFrom(other.sum)
    }
    override def readFields(in: DataInput): Unit = {
      id.readFields(in)
      sum.readFields(in)
    }
    override def write(out: DataOutput): Unit = {
      id.write(out)
      sum.write(out)
    }
  }

  object Foo {

    def intToFoo = new Function1[Int, (ShuffleKey, Foo)] with Serializable {

      @transient var f: Foo = _

      def foo: Foo = {
        if (f == null) {
          f = new Foo()
        }
        f
      }

      override def apply(i: Int): (ShuffleKey, Foo) = {
        foo.id.modify(i % 2)
        foo.sum.modify(i * 100)
        val shuffleKey = new ShuffleKey(
          WritableSerDe.serialize(foo.id), WritableSerDe.serialize(foo.sum))
        (shuffleKey, foo)
      }
    }
  }

  class TestAggregation(val mapSideCombine: Boolean)
    extends Aggregation[ShuffleKey, Foo, Foo] {

    override def newCombiner(): Foo = {
      new Foo()
    }

    override def initCombinerByValue(combiner: Foo, value: Foo): Foo = {
      combiner.copyFrom(value)
      combiner
    }

    override def mergeValue(combiner: Foo, value: Foo): Foo = {
      combiner.sum.add(value.sum)
      combiner
    }

    override def initCombinerByCombiner(comb1: Foo, comb2: Foo): Foo = {
      comb1.copyFrom(comb2)
      comb1
    }

    override def mergeCombiners(comb1: Foo, comb2: Foo): Foo = {
      comb1.sum.add(comb2.sum)
      comb1
    }
  }

  object TotalAggregate {

    val Result = BranchKey(0)

    class TestAggregateDriver(
      @transient sc: SparkContext,
      @transient hadoopConf: Broadcast[Configuration],
      @transient prev: Future[RDD[(ShuffleKey, Foo)]],
      @transient sort: Option[Ordering[ShuffleKey]],
      @transient part: Partitioner,
      val aggregation: Aggregation[ShuffleKey, Foo, Foo])
      extends AggregateDriver[Foo, Foo](sc, hadoopConf)(Seq(prev), sort, part)(Map.empty) {

      override def label = "TestAggregation"

      override def branchKeys: Set[BranchKey] = Set(Result)

      override def partitioners: Map[BranchKey, Option[Partitioner]] = Map.empty

      override def orderings: Map[BranchKey, Ordering[ShuffleKey]] = Map.empty

      override def aggregations: Map[BranchKey, Aggregation[ShuffleKey, _, _]] = Map.empty

      override def shuffleKey(branch: BranchKey, value: Any): ShuffleKey = {
        new ShuffleKey(WritableSerDe.serialize(value.asInstanceOf[Foo].id), Array.emptyByteArray)
      }

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
      @transient prev: Future[RDD[(_, Foo)]])
      extends ExtractDriver[Foo](sc, hadoopConf)(Seq(prev))(Map.empty) {

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
        new ShuffleKey(WritableSerDe.serialize(value.asInstanceOf[Foo].id), Array.emptyByteArray)
      }

      override def serialize(branch: BranchKey, value: Any): Array[Byte] = {
        WritableSerDe.serialize(value.asInstanceOf[Writable])
      }

      @transient var f: Foo = _

      def foo = {
        if (f == null) {
          f = new Foo()
        }
        f
      }

      override def deserialize(branch: BranchKey, value: Array[Byte]): Any = {
        WritableSerDe.deserialize(value, foo)
        foo
      }

      override def fragments(
        broadcasts: Map[BroadcastId, Broadcast[_]])(
          fragmentBufferSize: Int): (Fragment[Foo], Map[BranchKey, OutputFragment[_]]) = {
        val fragment1 = new GenericOutputFragment[Foo](fragmentBufferSize)
        val fragment2 = new GenericOutputFragment[Foo](fragmentBufferSize)
        (new GenericEdgeFragment[Foo](Array(fragment1, fragment2)),
          Map(
            Result1 -> fragment1,
            Result2 -> fragment2))
      }
    }
  }
}
