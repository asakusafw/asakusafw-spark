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
import scala.reflect.ClassTag

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.Writable
import org.apache.spark._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd._

import com.asakusafw.runtime.model.DataModel
import com.asakusafw.runtime.value.{ BooleanOption, IntOption }
import com.asakusafw.spark.runtime.aggregation.Aggregation
import com.asakusafw.spark.runtime.fragment._
import com.asakusafw.spark.runtime.io.WritableSerDe
import com.asakusafw.spark.runtime.rdd.BranchKey

@RunWith(classOf[JUnitRunner])
class CoGroupDriverSpecTest extends CoGroupDriverSpec

class CoGroupDriverSpec extends FlatSpec with SparkSugar {

  import CoGroupDriverSpec._

  behavior of "CoGroupDriver"

  it should "cogroup" in {
    val hogeOrd = new HogeSortOrdering()
    val fHoge = new Function1[Int, (ShuffleKey, Hoge)] with Serializable {

      @transient var h: Hoge = _

      def hoge: Hoge = {
        if (h == null) {
          h = new Hoge()
        }
        h
      }

      override def apply(i: Int): (ShuffleKey, Hoge) = {
        hoge.id.modify(i)
        val shuffleKey = new ShuffleKey(
          WritableSerDe.serialize(hoge.id),
          WritableSerDe.serialize(new BooleanOption().modify(hoge.id.get % 3 == 0)))
        (shuffleKey, hoge)
      }
    }
    val hoges = sc.parallelize(0 until 100).map(fHoge).asInstanceOf[RDD[(ShuffleKey, _)]]

    val fooOrd = new FooSortOrdering()
    val fFoo = new Function2[Int, Int, (ShuffleKey, Foo)] with Serializable {

      @transient var f: Foo = _

      def foo: Foo = {
        if (f == null) {
          f = new Foo()
        }
        f
      }

      override def apply(i: Int, j: Int): (ShuffleKey, Foo) = {
        foo.id.modify(100 + j)
        foo.hogeId.modify(i)
        val shuffleKey = new ShuffleKey(
          WritableSerDe.serialize(foo.hogeId),
          WritableSerDe.serialize(new IntOption().modify(foo.id.toString.hashCode)))
        (shuffleKey, foo)
      }
    }
    val foos = sc.parallelize(0 until 100).flatMap(i => (0 until i).iterator.map(fFoo(i, _))).asInstanceOf[RDD[(ShuffleKey, _)]]

    val grouping = new GroupingOrdering()
    val part = new HashPartitioner(2)
    val driver = new TestCoGroupDriver(
      sc, hadoopConf, Seq((Seq(Future.successful(hoges)), Option(hogeOrd)), (Seq(Future.successful(foos)), Option(fooOrd))), grouping, part)

    val outputs = driver.execute()

    val hogeResult = Await.result(
      outputs(HogeResult).map {
        _.map(_._2.asInstanceOf[Hoge]).map(_.id.get)
      }, Duration.Inf).collect.toSeq
    assert(hogeResult.size === 1)
    assert(hogeResult.head === 1)

    val fooResult = Await.result(
      outputs(FooResult).map {
        _.map(_._2.asInstanceOf[Foo]).map(foo => (foo.id.get, foo.hogeId.get))
      }, Duration.Inf).collect.toSeq
    assert(fooResult.size === 1)
    assert(fooResult.head._1 === 100)
    assert(fooResult.head._2 === 1)

    val hogeError = Await.result(
      outputs(HogeError).map {
        _.map(_._2.asInstanceOf[Hoge]).map(_.id.get)
      }, Duration.Inf).collect.toSeq.sorted
    assert(hogeError.size === 99)
    assert(hogeError.head === 0)
    for (i <- 2 until 10) {
      assert(hogeError(i - 1) === i)
    }

    val fooError = Await.result(
      outputs(FooError).map {
        _.map(_._2.asInstanceOf[Foo]).map(foo => (foo.id.get, foo.hogeId.get))
      }, Duration.Inf).collect.toSeq.sortBy(_._2)
    assert(fooError.size === 4949)
    for {
      i <- 2 until 100
      j <- 0 until i
    } {
      assert(fooError((i * (i - 1)) / 2 + j - 1)._1 === 100 + j)
      assert(fooError((i * (i - 1)) / 2 + j - 1)._2 === i)
    }
  }
}

object CoGroupDriverSpec {

  val HogeResult = BranchKey(0)
  val FooResult = BranchKey(1)
  val HogeError = BranchKey(2)
  val FooError = BranchKey(3)

  class TestCoGroupDriver(
    @transient sc: SparkContext,
    @transient hadoopConf: Broadcast[Configuration],
    @transient inputs: Seq[(Seq[Future[RDD[(ShuffleKey, _)]]], Option[Ordering[ShuffleKey]])],
    @transient grouping: Ordering[ShuffleKey],
    @transient part: Partitioner)
      extends CoGroupDriver(sc, hadoopConf, Map.empty, inputs, grouping, part) {

    override def label = "TestCoGroup"

    override def branchKeys: Set[BranchKey] = {
      Set(HogeResult, FooResult, HogeError, FooError)
    }

    override def partitioners: Map[BranchKey, Option[Partitioner]] = Map.empty

    override def orderings: Map[BranchKey, Ordering[ShuffleKey]] = Map.empty

    override def aggregations: Map[BranchKey, Aggregation[ShuffleKey, _, _]] = Map.empty

    override def shuffleKey(branch: BranchKey, value: Any): ShuffleKey = null

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

    @transient var f: Foo = _

    def foo = {
      if (f == null) {
        f = new Foo()
      }
      f
    }

    override def deserialize(branch: BranchKey, value: Array[Byte]): Any = {
      branch match {
        case HogeResult | HogeError =>
          WritableSerDe.deserialize(value, hoge)
          hoge
        case FooResult | FooError =>
          WritableSerDe.deserialize(value, foo)
          foo
      }
    }

    override def fragments(broadcasts: Map[BroadcastId, Broadcast[_]]): (Fragment[Seq[Iterator[_]]], Map[BranchKey, OutputFragment[_]]) = {
      val outputs = Map(
        HogeResult -> new HogeOutputFragment,
        FooResult -> new FooOutputFragment,
        HogeError -> new HogeOutputFragment,
        FooError -> new FooOutputFragment)
      val fragment = new TestCoGroupFragment(outputs)
      (fragment, outputs)
    }
  }

  class Hoge extends DataModel[Hoge] with Writable {

    val id = new IntOption()

    override def reset(): Unit = {
      id.setNull()
    }
    override def copyFrom(other: Hoge): Unit = {
      id.copyFrom(other.id)
    }
    override def readFields(in: DataInput): Unit = {
      id.readFields(in)
    }
    override def write(out: DataOutput): Unit = {
      id.write(out)
    }
  }

  class Foo extends DataModel[Foo] with Writable {

    val id = new IntOption()
    val hogeId = new IntOption()

    override def reset(): Unit = {
      id.setNull()
      hogeId.setNull()
    }
    override def copyFrom(other: Foo): Unit = {
      id.copyFrom(other.id)
      hogeId.copyFrom(other.hogeId)
    }
    override def readFields(in: DataInput): Unit = {
      id.readFields(in)
      hogeId.readFields(in)
    }
    override def write(out: DataOutput): Unit = {
      id.write(out)
      hogeId.write(out)
    }
  }

  class GroupingOrdering extends Ordering[ShuffleKey] {

    override def compare(x: ShuffleKey, y: ShuffleKey): Int = {
      val xGrouping = x.grouping
      val yGrouping = y.grouping
      IntOption.compareBytes(xGrouping, 0, xGrouping.length, yGrouping, 0, yGrouping.length)
    }
  }

  class HogeSortOrdering extends GroupingOrdering {

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

  class FooSortOrdering extends GroupingOrdering {

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

  class HogeOutputFragment extends OutputFragment[Hoge] {
    override def newDataModel: Hoge = new Hoge()
  }

  class FooOutputFragment extends OutputFragment[Foo] {
    override def newDataModel: Foo = new Foo()
  }

  class TestCoGroupFragment(outputs: Map[BranchKey, Fragment[_]]) extends Fragment[Seq[Iterator[_]]] {

    override def add(groups: Seq[Iterator[_]]): Unit = {
      assert(groups.size == 2)
      val hogeList = groups(0).asInstanceOf[Iterator[Hoge]].toSeq
      val fooList = groups(1).asInstanceOf[Iterator[Foo]].toSeq
      if (hogeList.size == 1 && fooList.size == 1) {
        outputs(HogeResult).asInstanceOf[HogeOutputFragment].add(hogeList.head)
        outputs(FooResult).asInstanceOf[FooOutputFragment].add(fooList.head)
      } else {
        hogeList.foreach(outputs(HogeError).asInstanceOf[HogeOutputFragment].add)
        fooList.foreach(outputs(FooError).asInstanceOf[FooOutputFragment].add)
      }
    }

    override def reset(): Unit = {
      outputs.values.foreach(_.reset())
    }
  }
}
