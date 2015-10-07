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
import org.apache.spark.{ HashPartitioner, Partitioner, SparkContext }
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

import com.asakusafw.runtime.model.DataModel
import com.asakusafw.runtime.value.{ BooleanOption, IntOption }
import com.asakusafw.spark.runtime.aggregation.Aggregation
import com.asakusafw.spark.runtime.fragment.{ Fragment, GenericOutputFragment, OutputFragment }
import com.asakusafw.spark.runtime.io.WritableSerDe
import com.asakusafw.spark.runtime.rdd.BranchKey

@RunWith(classOf[JUnitRunner])
class CoGroupDriverSpecTest extends CoGroupDriverSpec

class CoGroupDriverSpec extends FlatSpec with SparkForAll with HadoopConfForEach {

  import CoGroupDriverSpec._

  behavior of "CoGroupDriver"

  it should "cogroup" in {
    val foos = sc.parallelize(0 until 100)
      .map(Foo.intToFoo).asInstanceOf[RDD[(ShuffleKey, _)]]
    val fooOrd = new Foo.SortOrdering()

    val bars = sc.parallelize(0 until 100)
      .flatMap(i => (0 until i).iterator.map(Bar.intToBar(i, _))).asInstanceOf[RDD[(ShuffleKey, _)]]
    val barOrd = new Bar.SortOrdering()

    val grouping = new GroupingOrdering()
    val partitioner = new HashPartitioner(2)
    val driver = new TestCoGroupDriver(
      sc, hadoopConf,
      Seq(
        (Seq(Future.successful(foos)), Option(fooOrd)),
        (Seq(Future.successful(bars)), Option(barOrd))),
      grouping, partitioner)

    val outputs = driver.execute()

    val ((fooResult, barResult), (fooError, barError)) =
      Await.result(
        outputs(FooResult).map {
          _.map {
            case (_, foo: Foo) => foo.id.get
          }.collect.toSeq
        }.zip {
          outputs(BarResult).map {
            _.map {
              case (_, bar: Bar) => (bar.id.get, bar.fooId.get)
            }.collect.toSeq
          }
        }.zip {
          outputs(FooError).map {
            _.map {
              case (_, foo: Foo) => foo.id.get
            }.collect.toSeq.sorted
          }.zip {
            outputs(BarError).map {
              _.map {
                case (_, bar: Bar) => (bar.id.get, bar.fooId.get)
              }.collect.toSeq.sortBy(_._2)
            }
          }
        }, Duration.Inf)

    assert(fooResult.size === 1)
    assert(fooResult.head === 1)

    assert(barResult.size === 1)
    assert(barResult.head._1 === 100)
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
      assert(barError((i * (i - 1)) / 2 + j - 1)._1 === 100 + j)
      assert(barError((i * (i - 1)) / 2 + j - 1)._2 === i)
    }
  }
}

object CoGroupDriverSpec {

  class GroupingOrdering extends Ordering[ShuffleKey] {

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

    def intToFoo = new Function1[Int, (ShuffleKey, Foo)] with Serializable {

      @transient var f: Foo = _

      def foo: Foo = {
        if (f == null) {
          f = new Foo()
        }
        f
      }

      override def apply(i: Int): (ShuffleKey, Foo) = {
        foo.id.modify(i)
        val shuffleKey = new ShuffleKey(
          WritableSerDe.serialize(foo.id),
          WritableSerDe.serialize(new BooleanOption().modify(foo.id.get % 3 == 0)))
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

    def intToBar = new Function2[Int, Int, (ShuffleKey, Bar)] with Serializable {

      @transient var b: Bar = _

      def bar: Bar = {
        if (b == null) {
          b = new Bar()
        }
        b
      }

      override def apply(i: Int, j: Int): (ShuffleKey, Bar) = {
        bar.id.modify(100 + j)
        bar.fooId.modify(i)
        val shuffleKey = new ShuffleKey(
          WritableSerDe.serialize(bar.fooId),
          WritableSerDe.serialize(new IntOption().modify(bar.id.toString.hashCode)))
        (shuffleKey, bar)
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

  val FooResult = BranchKey(0)
  val BarResult = BranchKey(1)
  val FooError = BranchKey(2)
  val BarError = BranchKey(3)

  class TestCoGroupDriver(
    @transient sc: SparkContext,
    @transient hadoopConf: Broadcast[Configuration],
    @transient inputs: Seq[(Seq[Future[RDD[(ShuffleKey, _)]]], Option[Ordering[ShuffleKey]])],
    @transient grouping: Ordering[ShuffleKey],
    @transient part: Partitioner)
    extends CoGroupDriver(sc, hadoopConf)(inputs, grouping, part)(Map.empty) {

    override def label = "TestCoGroup"

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

    @transient var f: Foo = _

    def foo = {
      if (f == null) {
        f = new Foo()
      }
      f
    }

    @transient var b: Bar = _

    def bar = {
      if (b == null) {
        b = new Bar()
      }
      b
    }

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

    override def fragments(broadcasts: Map[BroadcastId, Broadcast[_]]): (Fragment[Seq[Iterator[_]]], Map[BranchKey, OutputFragment[_]]) = {
      val outputs = Map(
        FooResult -> new GenericOutputFragment[Foo](),
        BarResult -> new GenericOutputFragment[Bar](),
        FooError -> new GenericOutputFragment[Foo](),
        BarError -> new GenericOutputFragment[Bar]())
      val fragment = new TestCoGroupFragment(outputs)
      (fragment, outputs)
    }
  }

  class TestCoGroupFragment(outputs: Map[BranchKey, Fragment[_]]) extends Fragment[Seq[Iterator[_]]] {

    override def add(groups: Seq[Iterator[_]]): Unit = {
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

    override def reset(): Unit = {
      outputs.values.foreach(_.reset())
    }
  }
}
