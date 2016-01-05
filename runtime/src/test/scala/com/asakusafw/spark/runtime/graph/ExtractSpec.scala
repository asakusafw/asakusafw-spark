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

import scala.collection.JavaConversions._
import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration

import org.apache.hadoop.io.{ NullWritable, Writable }
import org.apache.spark.{ HashPartitioner, Partitioner, SparkConf, SparkContext }
import org.apache.spark.broadcast.{ Broadcast => Broadcasted }

import com.asakusafw.runtime.model.DataModel
import com.asakusafw.runtime.value.IntOption
import com.asakusafw.spark.runtime.aggregation.Aggregation
import com.asakusafw.spark.runtime.fixture.SparkForAll
import com.asakusafw.spark.runtime.fragment.{ Fragment, GenericOutputFragment, OutputFragment }
import com.asakusafw.spark.runtime.io.WritableSerDe
import com.asakusafw.spark.runtime.rdd.{ BranchKey, ShuffleKey }

@RunWith(classOf[JUnitRunner])
class ExtractSpecTest extends ExtractSpec

class ExtractSpec extends FlatSpec with SparkForAll with RoundContextSugar {

  import ExtractSpec._

  behavior of classOf[Extract[_]].getSimpleName

  for {
    numSlices <- Seq(None, Some(8), Some(4))
  } {
    val conf = s"numSlices = ${numSlices}"

    it should s"extract simply: [${conf}]" in { implicit sc =>
      import Simple._

      val source =
        new ParallelCollectionSource(Input, (0 until 100), numSlices)("input")
          .map(Input)(Foo.intToFoo)
      val extract =
        new SimpleExtract((source, Input))("extract")

      val rc = newRoundContext()

      val result = Await.result(
        extract.getOrCompute(rc).apply(Result).map {
          _.map {
            case (_, foo: Foo) => foo.id.get
          }.collect.toSeq
        }, Duration.Inf)
      assert(result.size === 100)
      assert(result === (0 until 100).map(i => i))
    }

    it should s"extract multiple prevs: [${conf}]" in { implicit sc =>
      import Simple._

      val source1 =
        new ParallelCollectionSource(Input, (0 until 50), numSlices)("input")
          .map(Input)(Foo.intToFoo)
      val source2 =
        new ParallelCollectionSource(Input, (50 until 100), numSlices)("input")
          .map(Input)(Foo.intToFoo)
      val extract =
        new SimpleExtract(Seq((source1, Input), (source2, Input)))("extract")

      val rc = newRoundContext()

      val result = Await.result(
        extract.getOrCompute(rc).apply(Result).map {
          _.map {
            case (_, foo: Foo) => foo.id.get
          }.collect.toSeq.sorted
        }, Duration.Inf)
      assert(result.size === 100)
      assert(result === (0 until 100).map(i => i))
    }

    it should s"extract with branch: [${conf}]" in { implicit sc =>
      import Branch._

      val source =
        new ParallelCollectionSource(Input, (0 until 100), numSlices)("input")
          .map(Input)(Foo.intToFoo)
      val branch =
        new BranchExtract(Seq((source, Input)))("branch")

      val rc = newRoundContext()

      val (result1, result2) = Await.result(
        branch.getOrCompute(rc).apply(Result1).map {
          _.map {
            case (_, foo: Foo) => foo.id.get
          }.collect.toSeq.sorted
        }.zip {
          branch.getOrCompute(rc).apply(Result2).map {
            _.map {
              case (_, foo: Foo) => foo.id.get
            }.collect.toSeq.sorted
          }
        }, Duration.Inf)

      assert(result1.size === 50)
      assert(result1 === (0 until 100 by 2).map(i => i))

      assert(result2.size === 50)
      assert(result2 === (1 until 100 by 2).map(i => i))
    }

    it should s"extract with branch and ordering: [${conf}]" in { implicit sc =>
      import BranchAndOrdering._

      val source =
        new ParallelCollectionSource(Input, (0 until 100))("input")
          .map(Input)(Bar.intToBar)
      val branch =
        new BranchAndOrderingExtract(Seq((source, Input)))("branchAndOrdering")

      val rc = newRoundContext()

      val (result1, result2) = Await.result(
        branch.getOrCompute(rc).apply(Result1).map {
          _.map {
            case (_, bar: Bar) => (bar.id.get, bar.ord.get)
          }.collect.toSeq
        }.zip {
          branch.getOrCompute(rc).apply(Result2).map {
            _.map {
              case (_, bar: Bar) => (bar.id.get, bar.ord.get)
            }.collect.toSeq
          }
        }, Duration.Inf)

      assert(result1.size === 40)
      assert(result1.map(_._1) === (0 until 100).map(_ % 5).filter(_ % 3 == 0))
      assert(result1.map(_._2) === (0 until 100).filter(i => (i % 5) % 3 == 0))

      assert(result2.size === 60)
      assert(result2 ===
        (0 until 100).filterNot(i => (i % 5) % 3 == 0)
        .map(i => (i % 5, i))
        .sortBy(t => (t._1, -t._2)))
    }
  }
}

@RunWith(classOf[JUnitRunner])
class ExtractWithParallelismSpecTest extends ExtractWithParallelismSpec

class ExtractWithParallelismSpec extends ExtractSpec {

  override def configure(conf: SparkConf): SparkConf = {
    conf.set("spark.default.parallelism", 8.toString)
  }
}

object ExtractSpec {

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
        (NullWritable.get, foo)
      }
    }
  }

  class Bar extends DataModel[Bar] with Writable {

    val id = new IntOption()
    val ord = new IntOption()

    override def reset(): Unit = {
      id.setNull()
      ord.setNull()
    }
    override def copyFrom(other: Bar): Unit = {
      id.copyFrom(other.id)
      ord.copyFrom(other.ord)
    }
    override def readFields(in: DataInput): Unit = {
      id.readFields(in)
      ord.readFields(in)
    }
    override def write(out: DataOutput): Unit = {
      id.write(out)
      ord.write(out)
    }
  }

  object Bar {

    def intToBar: Int => (_, Bar) = {

      lazy val bar = new Bar()

      { i =>
        bar.id.modify(i % 5)
        bar.ord.modify(i)
        (NullWritable.get, bar)
      }
    }
  }

  val Input = BranchKey(0)

  object Simple {

    val Result = BranchKey(1)

    class SimpleExtract(
      prevs: Seq[(Source, BranchKey)])(
        val label: String)(
          implicit sc: SparkContext)
      extends Extract[Foo](prevs)(Map.empty)
      with ComputeOnce {

      def this(
        prev: (Source, BranchKey))(
          label: String)(
            implicit sc: SparkContext) = this(Seq(prev))(label)

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

      override def fragments(
        broadcasts: Map[BroadcastId, Broadcasted[_]])(
          fragmentBufferSize: Int): (Fragment[Foo], Map[BranchKey, OutputFragment[_]]) = {
        val fragment = new GenericOutputFragment[Foo](fragmentBufferSize)
        (fragment, Map(Result -> fragment))
      }
    }
  }

  object Branch {

    val Result1 = BranchKey(1)
    val Result2 = BranchKey(2)

    class BranchExtract(
      prevs: Seq[(Source, BranchKey)])(
        val label: String)(
          implicit sc: SparkContext)
      extends Extract[Foo](prevs)(Map.empty)
      with ComputeOnce {

      override def branchKeys: Set[BranchKey] = Set(Result1, Result2)

      override def partitioners: Map[BranchKey, Option[Partitioner]] = Map.empty

      override def orderings: Map[BranchKey, Ordering[ShuffleKey]] = Map.empty

      override def aggregations: Map[BranchKey, Aggregation[ShuffleKey, _, _]] = Map.empty

      override def shuffleKey(branch: BranchKey, value: Any): ShuffleKey = null

      override def serialize(branch: BranchKey, value: Any): Array[Byte] = {
        WritableSerDe.serialize(value.asInstanceOf[Writable])
      }

      lazy val foo = new Foo()

      override def deserialize(branch: BranchKey, value: Array[Byte]): Any = {
        WritableSerDe.deserialize(value, foo)
        foo
      }

      override def fragments(
        broadcasts: Map[BroadcastId, Broadcasted[_]])(
          fragmentBufferSize: Int): (Fragment[Foo], Map[BranchKey, OutputFragment[_]]) = {
        val foo1Output = new GenericOutputFragment[Foo](fragmentBufferSize)
        val foo2Output = new GenericOutputFragment[Foo](fragmentBufferSize)
        val fragment = new BranchFragment(foo1Output, foo2Output)
        (fragment,
          Map(
            Result1 -> foo1Output,
            Result2 -> foo2Output))
      }
    }

    class BranchFragment(foo1Output: Fragment[Foo], foo2Output: Fragment[Foo]) extends Fragment[Foo] {

      override def add(foo: Foo): Unit = {
        if (foo.id.get % 2 == 0) {
          foo1Output.add(foo)
        } else {
          foo2Output.add(foo)
        }
      }

      override def reset(): Unit = {
        foo1Output.reset()
        foo2Output.reset()
      }
    }
  }

  object BranchAndOrdering {

    val Result1 = BranchKey(1)
    val Result2 = BranchKey(2)

    class BranchAndOrderingExtract(
      prevs: Seq[(Source, BranchKey)])(
        val label: String)(
          implicit sc: SparkContext)
      extends Extract[Bar](prevs)(Map.empty)
      with ComputeOnce {

      override def branchKeys: Set[BranchKey] = Set(Result1, Result2)

      override def partitioners: Map[BranchKey, Option[Partitioner]] =
        Map(Result2 -> Some(new HashPartitioner(1)))

      override def orderings: Map[BranchKey, Ordering[ShuffleKey]] =
        Map(Result2 -> new SortOrdering())

      override def aggregations: Map[BranchKey, Aggregation[ShuffleKey, _, _]] = Map.empty

      override def shuffleKey(branch: BranchKey, value: Any): ShuffleKey = {
        val bar = value.asInstanceOf[Bar]
        new ShuffleKey(
          WritableSerDe.serialize(bar.id),
          WritableSerDe.serialize(bar.ord))
      }

      override def serialize(branch: BranchKey, value: Any): Array[Byte] = {
        WritableSerDe.serialize(value.asInstanceOf[Writable])
      }

      lazy val bar = new Bar()

      override def deserialize(branch: BranchKey, value: Array[Byte]): Any = {
        WritableSerDe.deserialize(value, bar)
        bar
      }

      override def fragments(
        broadcasts: Map[BroadcastId, Broadcasted[_]])(
          fragmentBufferSize: Int): (Fragment[Bar], Map[BranchKey, OutputFragment[_]]) = {
        val bar1Output = new GenericOutputFragment[Bar](fragmentBufferSize)
        val bar2Output = new GenericOutputFragment[Bar](fragmentBufferSize)
        val fragment = new BranchFragment(bar1Output, bar2Output)
        (fragment,
          Map(
            Result1 -> bar1Output,
            Result2 -> bar2Output))
      }
    }

    class BranchFragment(bar1Output: Fragment[Bar], bar2Output: Fragment[Bar]) extends Fragment[Bar] {

      override def add(bar: Bar): Unit = {
        if (bar.id.get % 100 % 3 == 0) {
          bar1Output.add(bar)
        } else {
          bar2Output.add(bar)
        }
      }

      override def reset(): Unit = {
        bar1Output.reset()
        bar2Output.reset()
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
}
