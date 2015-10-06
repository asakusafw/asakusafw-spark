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
import org.apache.hadoop.io.{ NullWritable, Writable }
import org.apache.spark.{ HashPartitioner, Partitioner, SparkContext }
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

import com.asakusafw.runtime.model.DataModel
import com.asakusafw.runtime.value.{ BooleanOption, IntOption }
import com.asakusafw.spark.runtime.aggregation.Aggregation
import com.asakusafw.spark.runtime.fragment.{ Fragment, OutputFragment }
import com.asakusafw.spark.runtime.io.WritableSerDe
import com.asakusafw.spark.runtime.operator.GenericOutputFragment
import com.asakusafw.spark.runtime.rdd.BranchKey

@RunWith(classOf[JUnitRunner])
class ExtractDriverSpecTest extends ExtractDriverSpec

class ExtractDriverSpec extends FlatSpec with SparkForAll with HadoopConfForEach {

  import ExtractDriverSpec._

  behavior of classOf[ExtractDriver[_]].getSimpleName

  it should "map simply" in {
    import Simple._

    val foos = sc.parallelize(0 until 100).map(Foo.intToFoo)

    val driver = new SimpleExtractDriver(sc, hadoopConf, Future.successful(foos))

    val outputs = driver.execute()

    val result = Await.result(
      outputs(Result).map {
        _.map {
          case (_, foo: Foo) => foo.id.get
        }.collect.toSeq
      }, Duration.Inf)
    assert(result.size === 100)
    assert(result === (0 until 100))
  }

  it should "map with branch" in {
    import Branch._

    val foos = sc.parallelize(0 until 100).map(Foo.intToFoo)

    val driver = new BranchExtractDriver(sc, hadoopConf, Future.successful(foos))

    val outputs = driver.execute()

    val (result1, result2) =
      Await.result(
        outputs(Result1).map {
          _.map {
            case (_, foo: Foo) => foo.id.get
          }.collect.toSeq
        }.zip {
          outputs(Result2).map {
            _.map {
              case (_, foo: Foo) => foo.id.get
            }.collect.toSeq
          }
        }, Duration.Inf)

    assert(result1.size === 50)
    assert(result1 === (0 until 100 by 2))

    assert(result2.size === 50)
    assert(result2 === (1 until 100 by 2))
  }

  it should "map with branch and ordering" in {
    import BranchAndOrdering._

    val foos = sc.parallelize(0 until 100).map(Bar.intToBar)

    val driver = new BranchAndOrderingExtractDriver(sc, hadoopConf, Future.successful(foos))

    val outputs = driver.execute()

    val (result1, result2) =
      Await.result(
        outputs(Result1).map {
          _.map {
            case (_, bar: Bar) => (bar.id.get, bar.ord.get)
          }.collect.toSeq
        }.zip {
          outputs(Result2).map {
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
      (0 until 100).filterNot(i => (i % 5) % 3 == 0).map(i => (i % 5, i)).sortBy(t => (t._1, -t._2)))
  }
}

object ExtractDriverSpec {

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

    def intToFoo = new Function1[Int, (_, Foo)] with Serializable {

      @transient var f: Foo = _

      def foo: Foo = {
        if (f == null) {
          f = new Foo()
        }
        f
      }

      override def apply(i: Int): (_, Foo) = {
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

    def intToBar = new Function1[Int, (_, Bar)] with Serializable {

      @transient var b: Bar = _

      def bar: Bar = {
        if (b == null) {
          b = new Bar()
        }
        b
      }

      override def apply(i: Int): (_, Bar) = {
        bar.id.modify(i % 5)
        bar.ord.modify(i)
        (NullWritable.get, bar)
      }
    }
  }

  object Simple {

    val Result = BranchKey(0)

    class SimpleExtractDriver(
      @transient sc: SparkContext,
      @transient hadoopConf: Broadcast[Configuration],
      @transient prev: Future[RDD[(_, Foo)]])
      extends ExtractDriver[Foo](sc, hadoopConf)(Seq(prev))(Map.empty) {

      override def label = "SimpleMap"

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
        val output = new GenericOutputFragment[Foo]()
        val fragment = new SimpleFragment(output)
        (fragment, Map(Result -> output))
      }
    }

    class SimpleFragment(output: Fragment[Foo]) extends Fragment[Foo] {

      override def add(foo: Foo): Unit = {
        output.add(foo)
      }

      override def reset(): Unit = {
        output.reset()
      }
    }
  }

  object Branch {

    val Result1 = BranchKey(0)
    val Result2 = BranchKey(1)

    class BranchExtractDriver(
      @transient sc: SparkContext,
      @transient hadoopConf: Broadcast[Configuration],
      @transient prev: Future[RDD[(_, Foo)]])
      extends ExtractDriver[Foo](sc, hadoopConf)(Seq(prev))(Map.empty) {

      override def label = "BranchMap"

      override def branchKeys: Set[BranchKey] = Set(Result1, Result2)

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

      override def deserialize(branch: BranchKey, value: Array[Byte]): Any = {
        WritableSerDe.deserialize(value, foo)
        foo
      }

      override def fragments(broadcasts: Map[BroadcastId, Broadcast[_]]): (Fragment[Foo], Map[BranchKey, OutputFragment[_]]) = {
        val foo1Output = new GenericOutputFragment[Foo]()
        val foo2Output = new GenericOutputFragment[Foo]()
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

    val Result1 = BranchKey(0)
    val Result2 = BranchKey(1)

    class BranchAndOrderingExtractDriver(
      @transient sc: SparkContext,
      @transient hadoopConf: Broadcast[Configuration],
      @transient prev: Future[RDD[(_, Bar)]])
      extends ExtractDriver[Bar](sc, hadoopConf)(Seq(prev))(Map.empty) {

      override def label = "BranchAndOrderingMap"

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

      @transient var b: Bar = _

      def bar = {
        if (b == null) {
          b = new Bar()
        }
        b
      }

      override def deserialize(branch: BranchKey, value: Array[Byte]): Any = {
        WritableSerDe.deserialize(value, bar)
        bar
      }

      override def fragments(broadcasts: Map[BroadcastId, Broadcast[_]]): (Fragment[Bar], Map[BranchKey, OutputFragment[_]]) = {
        val bar1Output = new GenericOutputFragment[Bar]()
        val bar2Output = new GenericOutputFragment[Bar]()
        val fragment = new BranchFragment(bar1Output, bar2Output)
        (fragment,
          Map(
            Result1 -> bar1Output,
            Result2 -> bar2Output))
      }
    }

    class BranchFragment(bar1Output: Fragment[Bar], bar2Output: Fragment[Bar]) extends Fragment[Bar] {

      override def add(bar: Bar): Unit = {
        if (bar.id.get % 3 == 0) {
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
