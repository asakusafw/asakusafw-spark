/*
 * Copyright 2011-2016 Asakusa Framework Team.
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
import org.scalatest.fixture.FlatSpec
import org.scalatest.junit.JUnitRunner

import java.io.{ DataInput, DataOutput }

import scala.annotation.meta.param
import scala.collection.JavaConversions._
import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration

import org.apache.hadoop.io.Writable
import org.apache.spark.{ HashPartitioner, Partitioner, SparkConf, SparkContext }
import org.apache.spark.broadcast.{ Broadcast => Broadcasted }

import com.asakusafw.runtime.model.DataModel
import com.asakusafw.runtime.value.{ BooleanOption, IntOption }
import com.asakusafw.spark.runtime.aggregation.Aggregation
import com.asakusafw.spark.runtime.fixture.SparkForAll
import com.asakusafw.spark.runtime.fragment.{ Fragment, GenericOutputFragment, OutputFragment }
import com.asakusafw.spark.runtime.io.WritableSerDe
import com.asakusafw.spark.runtime.rdd.{ BranchKey, ShuffleKey }

@RunWith(classOf[JUnitRunner])
class CoGroupSpecTest extends CoGroupSpec

class CoGroupSpec extends FlatSpec with SparkForAll with RoundContextSugar {

  import CoGroupSpec._

  behavior of classOf[CoGroup].getSimpleName

  for {
    numSlices <- Seq(None, Some(8), Some(4))
  } {
    val conf = s"numSlices = ${numSlices}"

    it should s"cogroup: [${conf}]" in { implicit sc =>
      val foos =
        new ParallelCollectionSource(FooInput, (0 until 100), numSlices)("foos")
          .map(FooInput)(Foo.intToFoo)
      val fooOrd = new Foo.SortOrdering()

      val bars =
        new ParallelCollectionSource(BarInput, (0 until 100), numSlices)("foos")
          .flatMap(BarInput)(Bar.intToBars)
      val barOrd = new Bar.SortOrdering()

      val grouping = new GroupingOrdering()
      val partitioner = new HashPartitioner(2)

      val cogroup =
        new TestCoGroup(
          Seq(
            (Seq((foos, FooInput)), Option(fooOrd)),
            (Seq((bars, BarInput)), Option(barOrd))),
          grouping, partitioner)("cogroup")

      val rc = newRoundContext()

      val ((fooResult, barResult), (fooError, barError)) =
        Await.result(
          cogroup.getOrCompute(rc).apply(FooResult).map {
            _.map {
              case (_, foo: Foo) => foo.id.get
            }.collect.toSeq
          }.zip {
            cogroup.getOrCompute(rc).apply(BarResult).map {
              _.map {
                case (_, bar: Bar) => (bar.id.get, bar.fooId.get)
              }.collect.toSeq
            }
          }.zip {
            cogroup.getOrCompute(rc).apply(FooError).map {
              _.map {
                case (_, foo: Foo) => foo.id.get
              }.collect.toSeq.sorted
            }.zip {
              cogroup.getOrCompute(rc).apply(BarError).map {
                _.map {
                  case (_, bar: Bar) => (bar.id.get, bar.fooId.get)
                }.collect.toSeq.sortBy(_._2)
              }
            }
          }, Duration.Inf)

      assert(fooResult.size === 1)
      assert(fooResult.head === 1)

      assert(barResult.size === 1)
      assert(barResult.head._1 === 0)
      assert(barResult.head._2 === 1)

      assert(fooError.size === 99)
      assert(fooError.head === 0)
      for (i <- 2 until 10) {
        assert(fooError(i - 1) === i)
      }

      assert(barError.size === 4949)
      for {
        i <- 2 until 100
        j <- 0 until i
      } {
        assert(barError((i * (i - 1)) / 2 + j - 1)._1 === j)
        assert(barError((i * (i - 1)) / 2 + j - 1)._2 === i)
      }
    }

    it should s"cogroup multiple prevs: [${conf}]" in { implicit sc =>
      val foos1 =
        new ParallelCollectionSource(FooInput, (0 until 50), numSlices)("input")
          .map(FooInput)(Foo.intToFoo)
      val foos2 =
        new ParallelCollectionSource(FooInput, (50 until 100), numSlices)("input")
          .map(FooInput)(Foo.intToFoo)
      val fooOrd = new Foo.SortOrdering()

      val bars1 =
        new ParallelCollectionSource(BarInput, (0 until 50), numSlices)("input")
          .flatMap(BarInput)(Bar.intToBars)
      val bars2 =
        new ParallelCollectionSource(BarInput, (50 until 100), numSlices)("input")
          .flatMap(BarInput)(Bar.intToBars)
      val barOrd = new Bar.SortOrdering()

      val grouping = new GroupingOrdering()
      val partitioner = new HashPartitioner(2)

      val cogroup =
        new TestCoGroup(
          Seq(
            (Seq((foos1, FooInput), (foos2, FooInput)), Option(fooOrd)),
            (Seq((bars1, BarInput), (bars2, BarInput)), Option(barOrd))),
          grouping, partitioner)("cogroup")

      val rc = newRoundContext()

      val ((fooResult, barResult), (fooError, barError)) =
        Await.result(
          cogroup.getOrCompute(rc).apply(FooResult).map {
            _.map {
              case (_, foo: Foo) => foo.id.get
            }.collect.toSeq
          }.zip {
            cogroup.getOrCompute(rc).apply(BarResult).map {
              _.map {
                case (_, bar: Bar) => (bar.id.get, bar.fooId.get)
              }.collect.toSeq
            }
          }.zip {
            cogroup.getOrCompute(rc).apply(FooError).map {
              _.map {
                case (_, foo: Foo) => foo.id.get
              }.collect.toSeq.sorted
            }.zip {
              cogroup.getOrCompute(rc).apply(BarError).map {
                _.map {
                  case (_, bar: Bar) => (bar.id.get, bar.fooId.get)
                }.collect.toSeq.sortBy(_._2)
              }
            }
          }, Duration.Inf)

      assert(fooResult.size === 1)
      assert(fooResult.head === 1)

      assert(barResult.size === 1)
      assert(barResult.head._1 === 0)
      assert(barResult.head._2 === 1)

      assert(fooError.size === 99)
      assert(fooError.head === 0)
      for (i <- 2 until 10) {
        assert(fooError(i - 1) === i)
      }

      assert(barError.size === 4949)
      for {
        i <- 2 until 100
        j <- 0 until i
      } {
        assert(barError((i * (i - 1)) / 2 + j - 1)._1 === j)
        assert(barError((i * (i - 1)) / 2 + j - 1)._2 === i)
      }
    }
  }
}

@RunWith(classOf[JUnitRunner])
class CoGroupWithParallelismSpecTest extends CoGroupWithParallelismSpec

class CoGroupWithParallelismSpec extends CoGroupSpec {

  override def configure(conf: SparkConf): SparkConf = {
    conf.set("spark.default.parallelism", 8.toString)
  }
}

object CoGroupSpec {

  class GroupingOrdering extends GroupOrdering {

    override def compare(x: ShuffleKey, y: ShuffleKey): Int = {
      val xGrouping = x.grouping
      val yGrouping = y.grouping
      IntOption.compareBytes(xGrouping, 0, xGrouping.length, yGrouping, 0, yGrouping.length)
    }
  }

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
        val shuffleKey = new ShuffleKey(
          WritableSerDe.serialize(foo.id),
          WritableSerDe.serialize(new BooleanOption().modify(i % 3 == 0)))
        (shuffleKey, foo)
      }
    }

    class SortOrdering extends GroupingOrdering {

      override def compare(x: ShuffleKey, y: ShuffleKey): Int = {
        val cmp = super.compare(x, y)
        if (cmp == 0) {
          val xOrdering = x.ordering
          val yOrdering = y.ordering
          BooleanOption.compareBytes(xOrdering, 0, xOrdering.length, yOrdering, 0, yOrdering.length)
        } else {
          cmp
        }
      }
    }
  }

  class Bar extends DataModel[Bar] with Writable {

    val id = new IntOption()
    val fooId = new IntOption()

    override def reset(): Unit = {
      id.setNull()
      fooId.setNull()
    }
    override def copyFrom(other: Bar): Unit = {
      id.copyFrom(other.id)
      fooId.copyFrom(other.fooId)
    }
    override def readFields(in: DataInput): Unit = {
      id.readFields(in)
      fooId.readFields(in)
    }
    override def write(out: DataOutput): Unit = {
      id.write(out)
      fooId.write(out)
    }
  }

  object Bar {

    def intToBars: Int => Iterator[(_, Bar)] = {

      lazy val bar = new Bar()

      { i =>
        (0 until i).iterator.map { j =>
          bar.id.modify(j)
          bar.fooId.modify(i)
          val shuffleKey = new ShuffleKey(
            WritableSerDe.serialize(bar.fooId),
            WritableSerDe.serialize(new IntOption().modify(bar.id.toString.hashCode)))
          (shuffleKey, bar)
        }
      }
    }

    class SortOrdering extends GroupingOrdering {

      override def compare(x: ShuffleKey, y: ShuffleKey): Int = {
        val cmp = super.compare(x, y)
        if (cmp == 0) {
          val xOrdering = x.ordering
          val yOrdering = y.ordering
          IntOption.compareBytes(xOrdering, 0, xOrdering.length, yOrdering, 0, yOrdering.length)
        } else {
          cmp
        }
      }
    }
  }

  val FooInput = BranchKey(0)
  val BarInput = BranchKey(1)

  val FooResult = BranchKey(2)
  val BarResult = BranchKey(3)
  val FooError = BranchKey(4)
  val BarError = BranchKey(5)

  class TestCoGroup(
    @(transient @param) inputs: Seq[(Seq[(Source, BranchKey)], Option[SortOrdering])],
    @(transient @param) grouping: GroupOrdering,
    @(transient @param) part: Partitioner)(
      val label: String)(
        implicit sc: SparkContext)
    extends CoGroup(inputs, grouping, part)(Map.empty)
    with ComputeOnce {

    override def branchKeys: Set[BranchKey] = {
      Set(FooResult, BarResult, FooError, BarError)
    }

    override def partitioners: Map[BranchKey, Option[Partitioner]] = Map.empty

    override def orderings: Map[BranchKey, Ordering[ShuffleKey]] = Map.empty

    override def aggregations: Map[BranchKey, Aggregation[ShuffleKey, _, _]] = Map.empty

    override def shuffleKey(branch: BranchKey, value: Any): ShuffleKey = null

    override def serialize(branch: BranchKey, value: Any): Array[Byte] = {
      WritableSerDe.serialize(value.asInstanceOf[Writable])
    }

    lazy val foo = new Foo()
    lazy val bar = new Bar()

    override def deserialize(branch: BranchKey, value: Array[Byte]): Any = {
      branch match {
        case FooResult | FooError =>
          WritableSerDe.deserialize(value, foo)
          foo
        case BarResult | BarError =>
          WritableSerDe.deserialize(value, bar)
          bar
      }
    }

    override def fragments(
      broadcasts: Map[BroadcastId, Broadcasted[_]])(
        fragmentBufferSize: Int): (Fragment[IndexedSeq[Iterator[_]]], Map[BranchKey, OutputFragment[_]]) = {
      val outputs = Map(
        FooResult -> new GenericOutputFragment[Foo](fragmentBufferSize),
        BarResult -> new GenericOutputFragment[Bar](fragmentBufferSize),
        FooError -> new GenericOutputFragment[Foo](fragmentBufferSize),
        BarError -> new GenericOutputFragment[Bar](fragmentBufferSize))
      val fragment = new TestCoGroupFragment(outputs)
      (fragment, outputs)
    }
  }

  class TestCoGroupFragment(outputs: Map[BranchKey, Fragment[_]]) extends Fragment[IndexedSeq[Iterator[_]]] {

    override def doAdd(groups: IndexedSeq[Iterator[_]]): Unit = {
      assert(groups.size == 2)
      val fooList = groups(0).asInstanceOf[Iterator[Foo]].toSeq
      val barList = groups(1).asInstanceOf[Iterator[Bar]].toSeq
      if (fooList.size == 1 && barList.size == 1) {
        outputs(FooResult).asInstanceOf[OutputFragment[Foo]].add(fooList.head)
        outputs(BarResult).asInstanceOf[OutputFragment[Bar]].add(barList.head)
      } else {
        fooList.foreach(outputs(FooError).asInstanceOf[OutputFragment[Foo]].add)
        barList.foreach(outputs(BarError).asInstanceOf[OutputFragment[Bar]].add)
      }
    }

    override def doReset(): Unit = {
      outputs.values.foreach(_.reset())
    }
  }
}
