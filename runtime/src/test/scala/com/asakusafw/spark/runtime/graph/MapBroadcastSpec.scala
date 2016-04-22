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

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration

import org.apache.hadoop.io.Writable
import org.apache.spark.HashPartitioner
import org.apache.spark.broadcast.{ Broadcast => Broadcasted }

import com.asakusafw.runtime.model.DataModel
import com.asakusafw.runtime.value.{ IntOption, StringOption }
import com.asakusafw.spark.runtime.fixture.SparkForAll
import com.asakusafw.spark.runtime.io.WritableSerDe
import com.asakusafw.spark.runtime.rdd.{ BranchKey, ShuffleKey }

@RunWith(classOf[JUnitRunner])
class MapBroadcastSpecTest extends MapBroadcastSpec

class MapBroadcastSpec extends FlatSpec with SparkForAll with RoundContextSugar {

  import MapBroadcastSpec._

  behavior of classOf[MapBroadcast].getSimpleName

  it should "broadcast as map" in { implicit sc =>
    val source =
      new ParallelCollectionSource[(Int, String)](Input,
        (0 until 10).flatMap { i =>
          (0 to i).map { j =>
            (((i * (i + 1)) / 2 + j, "%02d".format(i)), math.random)
          }
        }.sortBy(_._2).map(_._1))("input")
        .map[(Int, String), (_, Foo)](Input)(Foo.intToFoo)

    val broadcast = new MapBroadcastOnce(
      source,
      Input,
      None,
      new GroupingOrdering(),
      new HashPartitioner(1))("broadcast")

    val rc = newRoundContext()

    val result = Await.result(
      broadcast.getOrBroadcast(rc).map(_.asInstanceOf[Broadcasted[Map[ShuffleKey, Seq[Foo]]]]),
      Duration.Inf).value
    assert(result.size === 10)
    (0 until 10).foreach { i =>
      val key = new StringOption()
      key.modify("%02d".format(i))
      val shuffleKey = new ShuffleKey(
        WritableSerDe.serialize(key),
        Array.emptyByteArray)
      assert(result(shuffleKey).map(_.id.get).sorted === (0 to i).map((i * (i + 1)) / 2 + _))
    }
  }

  it should "broadcast as map with sort" in { implicit sc =>
    val source =
      new ParallelCollectionSource[(Int, String)](Input,
        (0 until 10).flatMap { i =>
          (0 to i).map { j =>
            (((i * (i + 1)) / 2 + j, "%02d".format(i)), math.random)
          }
        }.sortBy(_._2).map(_._1))("input")
        .map[(Int, String), (_, Foo)](Input)(Foo.intToFoo)

    val broadcast = new MapBroadcastOnce(
      source,
      Input,
      Some(new SortOrdering()),
      new GroupingOrdering(),
      new HashPartitioner(1))("broadcast")

    val rc = newRoundContext()

    val result = Await.result(
      broadcast.getOrBroadcast(rc).map(_.asInstanceOf[Broadcasted[Map[ShuffleKey, Seq[Foo]]]]),
      Duration.Inf).value
    assert(result.size === 10)
    (0 until 10).foreach { i =>
      val key = new StringOption()
      key.modify("%02d".format(i))
      val shuffleKey = new ShuffleKey(
        WritableSerDe.serialize(key),
        Array.emptyByteArray)
      assert(result(shuffleKey).map(_.id.get) === (0 to i).reverse.map((i * (i + 1)) / 2 + _))
    }
  }
}

object MapBroadcastSpec {

  class Foo extends DataModel[Foo] with Writable {

    val id = new IntOption()
    val key = new StringOption()

    override def reset(): Unit = {
      id.setNull()
      key.setNull()
    }
    override def copyFrom(other: Foo): Unit = {
      id.copyFrom(other.id)
      key.copyFrom(other.key)
    }
    override def readFields(in: DataInput): Unit = {
      id.readFields(in)
      key.readFields(in)
    }
    override def write(out: DataOutput): Unit = {
      id.write(out)
      key.write(out)
    }

    def getIdOption: IntOption = id
    def getKeyOption: StringOption = key
  }

  object Foo {

    def intToFoo: ((Int, String)) => (_, Foo) = {

      lazy val foo = new Foo()

      {
        case (id, key) =>
          foo.id.modify(id)
          foo.key.modify(key)
          val shuffleKey = new ShuffleKey(
            WritableSerDe.serialize(foo.key),
            WritableSerDe.serialize(foo.id))
          (shuffleKey, foo)
      }
    }
  }

  val Input = BranchKey(0)

  class GroupingOrdering extends Ordering[ShuffleKey] {

    override def compare(x: ShuffleKey, y: ShuffleKey): Int = {
      val xGrouping = x.grouping
      val yGrouping = y.grouping
      StringOption.compareBytes(xGrouping, 0, xGrouping.length, yGrouping, 0, yGrouping.length)
    }
  }

  class SortOrdering extends GroupingOrdering {

    override def compare(x: ShuffleKey, y: ShuffleKey): Int = {
      val cmp = super.compare(x, y)
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
