/*
 * Copyright 2011-2018 Asakusa Framework Team.
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
import org.scalatest.FlatSpec
import org.scalatest.junit.JUnitRunner

import java.io.{ DataInput, DataOutput }

import scala.collection.JavaConversions._
import scala.concurrent.{ Await, Future }
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration

import org.apache.hadoop.io.Writable
import org.apache.spark.{ HashPartitioner, Partitioner, SparkConf }
import org.apache.spark.broadcast.{ Broadcast => Broadcasted }
import org.apache.spark.rdd.RDD

import com.asakusafw.bridge.api.BatchContext
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
import com.asakusafw.spark.runtime.rdd.{ BranchKey, ShuffleKey }

@RunWith(classOf[JUnitRunner])
class AggregateSpecTest extends AggregateSpec

class AggregateSpec
  extends FlatSpec
  with SparkForAll
  with JobContextSugar
  with RoundContextSugar {

  import AggregateSpec._

  behavior of classOf[Aggregate[_, _]].getSimpleName

  for {
    numSlices <- Seq(None, Some(8), Some(4))
    mapSideCombine <- Seq(true, false)
  } {
    val conf = s"numSlices = ${numSlices}, mapSideCombine = ${mapSideCombine} "

    it should s"aggregate: [${conf}]" in {
      import TotalAggregate._

      implicit val jobContext = newJobContext(sc)

      val foos =
        new ParallelCollectionSource(Input, (0 until 10), numSlices)("foos")
          .map(Input)(Foo.intToFoo)

      val sort = Option(new SortOrdering())
      val partitioner = new HashPartitioner(2)
      val aggregation = new TestAggregation()

      val aggregate =
        new TestAggregate(
          (foos, Input),
          sort, partitioner, mapSideCombine, aggregation)("aggregate")

      val rc = newRoundContext(batchArguments = Map("bias" -> 0.toString))

      val result = Await.result(
        aggregate.compute(rc).apply(Result).map {
          _().map {
            case (_, foo: Foo) => (foo.id.get, foo.sum.get)
          }.collect.toSeq.sortBy(_._1)
        }, Duration.Inf)
      assert(result === Seq(
        (0, (0 until 10 by 2).map(i => i * 100).sum),
        (1, (1 until 10 by 2).map(i => i * 100).sum)))
    }

    it should s"aggregate multiple prevs: [${conf}]" in {
      import TotalAggregate._

      implicit val jobContext = newJobContext(sc)

      val foos1 =
        new ParallelCollectionSource(Input, (0 until 5), numSlices)("foos1")
          .map(Input)(Foo.intToFoo)
      val foos2 =
        new ParallelCollectionSource(Input, (5 until 10), numSlices)("foos2")
          .map(Input)(Foo.intToFoo)

      val sort = Option(new SortOrdering())
      val partitioner = new HashPartitioner(2)
      val aggregation = new TestAggregation()

      val aggregate =
        new TestAggregate(
          Seq((foos1, Input), (foos2, Input)),
          sort, partitioner, mapSideCombine, aggregation)("aggregate")

      val rc = newRoundContext(batchArguments = Map("bias" -> 0.toString))

      val result = Await.result(
        aggregate.compute(rc).apply(Result).map {
          _().map {
            case (_, foo: Foo) => (foo.id.get, foo.sum.get)
          }.collect.toSeq.sortBy(_._1)
        }, Duration.Inf)

      assert(result ===
        Seq((0, (0 until 10 by 2).map(_ * 100).sum), (1, (1 until 10 by 2).map(_ * 100).sum)))
    }
  }

  it should s"aggregate partially" in {
    import PartialAggregate._

    implicit val jobContext = newJobContext(sc)

    val foos =
      new ParallelCollectionSource(Input, (0 until 10), Option(2))("foos")
        .map(Input)(Foo.intToFoo)

    val aggregate =
      new TestPartialAggregationExtract((foos, Input))("partial-aggregate")

    val rc = newRoundContext(batchArguments = Map("bias" -> 0.toString))

    val (result1, result2) = Await.result(
      aggregate.compute(rc).apply(Result1).map {
        _().map {
          case (_, foo: Foo) => (foo.id.get, foo.sum.get)
        }.collect.toSeq.sortBy(_._1)
      }.zip {
        aggregate.compute(rc).apply(Result2).map {
          _().map {
            case (_, foo: Foo) => foo.sum.get
          }.collect.toSeq
        }
      }, Duration.Inf)

    assert(result1 ===
      Seq(
        (0, (0 until 10).filter(_ % 2 == 0).filter(_ < 5).map(i => i * 100).sum),
        (0, (0 until 10).filter(_ % 2 == 0).filterNot(_ < 5).map(i => i * 100).sum),
        (1, (0 until 10).filter(_ % 2 == 1).filter(_ < 5).map(i => i * 100).sum),
        (1, (0 until 10).filter(_ % 2 == 1).filterNot(_ < 5).map(i => i * 100).sum)))
    assert(result2 === (0 until 10).map(i => i * 100))
  }
}

@RunWith(classOf[JUnitRunner])
class AggregateWithParallelismSpecTest extends AggregateWithParallelismSpec

class AggregateWithParallelismSpec extends AggregateSpec {

  override def configure(conf: SparkConf): SparkConf = {
    conf.set("spark.default.parallelism", 8.toString)
  }
}

object AggregateSpec {

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

    def intToFoo: Int => (_, Foo) = {

      lazy val foo = new Foo()

      { i =>
        foo.id.modify(i % 2)
        foo.sum.modify(i * 100)
        val shuffleKey = new ShuffleKey(
          WritableSerDe.serialize(foo.id), WritableSerDe.serialize(foo.sum))
        (shuffleKey, foo)
      }
    }
  }

  class TestAggregation extends Aggregation[ShuffleKey, Foo, Foo] {

    override def newCombiner(): Foo = {
      new Foo()
    }

    override def initCombinerByValue(combiner: Foo, value: Foo): Foo = {
      combiner.copyFrom(value)
      combiner
    }

    override def mergeValue(combiner: Foo, value: Foo): Foo = {
      val bias = BatchContext.get("bias").toInt
      combiner.sum.add(value.sum)
      combiner.sum.add(bias)
      combiner
    }

    override def initCombinerByCombiner(comb1: Foo, comb2: Foo): Foo = {
      comb1.copyFrom(comb2)
      comb1
    }

    override def mergeCombiners(comb1: Foo, comb2: Foo): Foo = {
      val bias = BatchContext.get("bias").toInt
      comb1.sum.add(comb2.sum)
      comb1.sum.add(bias)
      comb1
    }
  }

  val Input = BranchKey(0)

  object TotalAggregate {

    val Result = BranchKey(1)

    class TestAggregate(
      prev: Seq[(Source, BranchKey)],
      sort: Option[SortOrdering],
      part: Partitioner,
      val mapSideCombine: Boolean,
      _aggregation: Aggregation[ShuffleKey, Foo, Foo])(
        val label: String)(
          implicit jobContext: JobContext)
      extends Aggregate[Foo, Foo](prev, sort, part)(Map.empty)
      with CacheOnce[RoundContext, Map[BranchKey, Future[() => RDD[_]]]] {

      def this(
        prev: (Source, BranchKey),
        sort: Option[SortOrdering],
        part: Partitioner,
        mapSideCombine: Boolean,
        aggregation: Aggregation[ShuffleKey, Foo, Foo])(
          label: String)(
            implicit jobContext: JobContext) =
        this(Seq(prev), sort, part, mapSideCombine, aggregation)(label)

      override def aggregation(
        broadcasts: Map[BroadcastId, Broadcasted[_]]): Aggregation[ShuffleKey, Foo, Foo] = _aggregation

      override def branchKeys: Set[BranchKey] = Set(Result)

      override def partitioners: Map[BranchKey, Option[Partitioner]] = Map.empty

      override def orderings: Map[BranchKey, Ordering[ShuffleKey]] = Map.empty

      override def aggregations(
        broadcasts: Map[BroadcastId, Broadcasted[_]]): Map[BranchKey, Aggregation[ShuffleKey, _, _]] = Map.empty

      override def shuffleKey(branch: BranchKey, value: Any): ShuffleKey = {
        new ShuffleKey(WritableSerDe.serialize(value.asInstanceOf[Foo].id), Array.emptyByteArray)
      }

      override def deserializerFor(branch: BranchKey): Array[Byte] => Any = { value =>
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
      prev: (Source, BranchKey))(
        val label: String)(
          implicit jobContext: JobContext)
      extends Extract[Foo](Seq(prev))(Map.empty)
      with CacheOnce[RoundContext, Map[BranchKey, Future[() => RDD[_]]]] {

      override def branchKeys: Set[BranchKey] = Set(Result1, Result2)

      override def partitioners: Map[BranchKey, Option[Partitioner]] = {
        Map(Result1 -> Some(new HashPartitioner(2)))
      }

      override def orderings: Map[BranchKey, Ordering[ShuffleKey]] = Map.empty

      override def aggregations(
        broadcasts: Map[BroadcastId, Broadcasted[_]]): Map[BranchKey, Aggregation[ShuffleKey, _, _]] = {
        Map(Result1 -> new TestAggregation())
      }

      override def shuffleKey(branch: BranchKey, value: Any): ShuffleKey = {
        new ShuffleKey(WritableSerDe.serialize(value.asInstanceOf[Foo].id), Array.emptyByteArray)
      }

      lazy val foo = new Foo()

      override def deserializerFor(branch: BranchKey): Array[Byte] => Any = { value =>
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
