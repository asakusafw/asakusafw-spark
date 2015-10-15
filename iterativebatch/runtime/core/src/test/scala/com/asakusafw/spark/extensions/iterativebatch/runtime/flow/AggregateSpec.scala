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

import java.io.{ DataInput, DataOutput }

import scala.collection.JavaConversions._
import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.reflect.ClassTag

import org.apache.hadoop.io.Writable
import org.apache.spark.{ HashPartitioner, Partitioner, SparkContext }
import org.apache.spark.broadcast.{ Broadcast => Broadcasted }

import com.asakusafw.bridge.stage.StageInfo
import com.asakusafw.runtime.model.DataModel
import com.asakusafw.runtime.value.IntOption
import com.asakusafw.spark.runtime.aggregation.Aggregation
import com.asakusafw.spark.runtime.driver.{ BroadcastId, ShuffleKey }
import com.asakusafw.spark.runtime.fragment.{
  Fragment,
  GenericEdgeFragment,
  GenericOutputFragment,
  OutputFragment
}
import com.asakusafw.spark.runtime.io.WritableSerDe
import com.asakusafw.spark.runtime.rdd._

import com.asakusafw.spark.extensions.iterativebatch.runtime.fixture.SparkForAll

@RunWith(classOf[JUnitRunner])
class AggregateSpecTest extends AggregateSpec

class AggregateSpec extends FlatSpec with SparkForAll with RoundContextSugar {

  import AggregateSpec._

  behavior of classOf[Aggregate[_, _]].getSimpleName

  for {
    mapSideCombine <- Seq(true, false)
  } {
    it should s"aggregate with map-side combine = ${mapSideCombine}" in { implicit sc =>
      import TotalAggregate._

      val source =
        new ParallelCollectionSource(Input, (0 until 10))("input")
          .mapWithRoundContext(Input)(Foo.intToFoo)

      val sort = Option(new SortOrdering())
      val partitioner = new HashPartitioner(2)
      val aggregation = new TestAggregation(mapSideCombine)

      val aggregate =
        new TestAggregate((source, Input), sort, partitioner, aggregation)("aggregate")

      for {
        round <- 0 to 1
      } {
        val rc = newRoundContext(batchArguments = Map("round" -> round.toString))

        val result = Await.result(
          aggregate.getOrCompute(rc).apply(Result).map {
            _.map {
              case (_, foo: Foo) => (foo.id.get, foo.price.get)
            }.collect.toSeq.sortBy(_._1)
          }, Duration.Inf)
        assert(result === Seq(
          (100 * round + 0, (0 until 10 by 2).map(i => 100 * round + i * 100).sum),
          (100 * round + 1, (1 until 10 by 2).map(i => 100 * round + i * 100).sum)))
      }
    }
  }

  it should "aggregate partially" in { implicit sc =>
    import PartialAggregate._

    val source =
      new ParallelCollectionSource(Input, (0 until 10), Option(2))("input")
        .mapWithRoundContext(Input)(Foo.intToFoo)

    val aggregate = new TestPartialAggregationExtract((source, Input))("partial-aggregate")

    for {
      round <- 0 to 1
    } {
      val rc = newRoundContext(batchArguments = Map("round" -> round.toString))

      val (result1, result2) = Await.result(
        aggregate.getOrCompute(rc).apply(Result1).map {
          _.map {
            case (_, foo: Foo) => (foo.id.get, foo.price.get)
          }.collect.toSeq.sortBy(_._1)
        }.zip {
          aggregate.getOrCompute(rc).apply(Result2).map {
            _.map {
              case (_, foo: Foo) => foo.price.get
            }.collect.toSeq
          }
        }, Duration.Inf)

      assert(result1 ===
        Seq(
          (100 * round + 0,
            (0 until 10).filter(_ % 2 == 0).filter(_ < 5).map(i => 100 * round + i * 100).sum),
          (100 * round + 0,
            (0 until 10).filter(_ % 2 == 0).filterNot(_ < 5).map(i => 100 * round + i * 100).sum),
          (100 * round + 1,
            (0 until 10).filter(_ % 2 == 1).filter(_ < 5).map(i => 100 * round + i * 100).sum),
          (100 * round + 1,
            (0 until 10).filter(_ % 2 == 1).filterNot(_ < 5).map(i => 100 * round + i * 100).sum)))
      assert(result2 === (0 until 10).map(i => 100 * round + i * 100))
    }
  }
}

object AggregateSpec {

  class Foo extends DataModel[Foo] with Writable {

    val id = new IntOption()
    val price = new IntOption()

    override def reset(): Unit = {
      id.setNull()
      price.setNull()
    }
    override def copyFrom(other: Foo): Unit = {
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

  object Foo {

    val intToFoo = { rc: RoundContext =>

      val stageInfo = StageInfo.deserialize(rc.hadoopConf.value.get(StageInfo.KEY_NAME))
      val round = stageInfo.getBatchArguments()("round").toInt

      new Function1[Int, (ShuffleKey, Foo)] with Serializable {

        @transient var f: Foo = _

        def foo: Foo = {
          if (f == null) {
            f = new Foo()
          }
          f
        }

        override def apply(i: Int): (ShuffleKey, Foo) = {
          foo.id.modify(100 * round + (i % 2))
          foo.price.modify(100 * round + i * 100)
          val shuffleKey = new ShuffleKey(
            WritableSerDe.serialize(foo.id), WritableSerDe.serialize(foo.price))
          (shuffleKey, foo)
        }
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
      combiner.price.add(value.price)
      combiner
    }

    override def initCombinerByCombiner(comb1: Foo, comb2: Foo): Foo = {
      comb1.copyFrom(comb2)
      comb1
    }

    override def mergeCombiners(comb1: Foo, comb2: Foo): Foo = {
      comb1.price.add(comb2.price)
      comb1
    }
  }

  val Input = BranchKey(0)

  object TotalAggregate {

    val Result = BranchKey(1)

    class TestAggregate(
      prev: Target,
      sort: Option[SortOrdering],
      part: Partitioner,
      val aggregation: Aggregation[ShuffleKey, Foo, Foo])(
        val label: String)(
          implicit sc: SparkContext)
      extends Aggregate[Foo, Foo](Seq(prev), sort, part)(Map.empty) {

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
        broadcasts: Map[BroadcastId, Broadcasted[_]])(
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

    val Result1 = BranchKey(1)
    val Result2 = BranchKey(2)

    class TestPartialAggregationExtract(
      @transient prev: Target)(
        val label: String)(
          implicit sc: SparkContext)
      extends Extract[Foo](Seq(prev))(Map.empty) {

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
        broadcasts: Map[BroadcastId, Broadcasted[_]])(
          fragmentBufferSize: Int): (Fragment[Foo], Map[BranchKey, OutputFragment[_]]) = {
        val fragment1 = new GenericOutputFragment[Foo](fragmentBufferSize)
        val fragment2 = new GenericOutputFragment[Foo](fragmentBufferSize)
        (new GenericEdgeFragment[Foo](Array(fragment1, fragment2)), Map(
          Result1 -> fragment1,
          Result2 -> fragment2))
      }
    }
  }
}
