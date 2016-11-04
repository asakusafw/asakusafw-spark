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
import org.scalatest.FlatSpec
import org.scalatest.junit.JUnitRunner

import java.io.{ DataInput, DataOutput }

import scala.concurrent.{ Await, Future }
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration

import org.apache.hadoop.io.{ NullWritable, Writable }
import org.apache.spark.Partitioner
import org.apache.spark.broadcast.{ Broadcast => Broadcasted }
import org.apache.spark.rdd.RDD

import com.asakusafw.bridge.api.BatchContext
import com.asakusafw.runtime.model.DataModel
import com.asakusafw.runtime.value.{ IntOption, StringOption }
import com.asakusafw.spark.runtime.aggregation.Aggregation
import com.asakusafw.spark.runtime.fragment.{ Fragment, GenericOutputFragment, OutputFragment }
import com.asakusafw.spark.runtime.rdd.{ BranchKey, ShuffleKey }

@RunWith(classOf[JUnitRunner])
class ResourceBrokingIteratorSpecTest extends ResourceBrokingIteratorSpec

class ResourceBrokingIteratorSpec
  extends FlatSpec
  with SparkForAll
  with JobContextSugar
  with RoundContextSugar {

  import ResourceBrokingIteratorSpec._

  behavior of classOf[ResourceBrokingIterator[_]].getSimpleName

  it should "broke resources" in {
    implicit val jobContext = newJobContext(sc)

    val source =
      new ParallelCollectionSource(Input, (0 until 10))("input")
        .map(Input)(Foo.intToFoo)

    val extract = new TestExtract((source, Input))("")

    val rc = newRoundContext(batchArguments = Map("batcharg" -> "test"))

    val result = Await.result(
      extract.compute(rc).apply(Result).map {
        _().map {
          case (_, foo: Foo) => (foo.id.get, foo.str.getAsString)
        }.collect.toSeq
      }, Duration.Inf)
    assert(result.size === 10)
    assert(result.map(_._1) === (0 until 10))
    assert(result.map(_._2) === (0 until 10).map(i => s"test_${i}"))
  }
}

object ResourceBrokingIteratorSpec {

  class Foo extends DataModel[Foo] with Writable {

    val id = new IntOption()
    val str = new StringOption()

    override def reset(): Unit = {
      id.setNull()
      str.setNull()
    }
    override def copyFrom(other: Foo): Unit = {
      id.copyFrom(other.id)
      str.copyFrom(other.str)
    }
    override def readFields(in: DataInput): Unit = {
      id.readFields(in)
      str.readFields(in)
    }
    override def write(out: DataOutput): Unit = {
      id.write(out)
      str.write(out)
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

  val Input = BranchKey(0)
  val Result = BranchKey(1)

  class TestExtract(
    prevs: Seq[(Source, BranchKey)])(
      val label: String)(
        implicit jobContext: JobContext)
    extends Extract[Foo](prevs)(Map.empty)
    with CacheOnce[RoundContext, Map[BranchKey, Future[() => RDD[_]]]] {

    def this(
      prev: (Source, BranchKey))(
        label: String)(
          implicit jobContext: JobContext) = this(Seq(prev))(label)

    override def branchKeys: Set[BranchKey] = Set(Result)

    override def partitioners: Map[BranchKey, Option[Partitioner]] = Map.empty

    override def orderings: Map[BranchKey, Ordering[ShuffleKey]] = Map.empty

    override def aggregations: Map[BranchKey, Aggregation[ShuffleKey, _, _]] = Map.empty

    override def shuffleKey(branch: BranchKey, value: Any): ShuffleKey = null

    override def deserializerFor(branch: BranchKey): Array[Byte] => Any = { value =>
      ???
    }

    override def fragments(
      broadcasts: Map[BroadcastId, Broadcasted[_]])(
        fragmentBufferSize: Int): (Fragment[Foo], Map[BranchKey, OutputFragment[_]]) = {
      val outputs = Map(
        Result -> new GenericOutputFragment[Foo](fragmentBufferSize))
      val fragment = new TestFragment(outputs(Result))
      (fragment, outputs)
    }
  }

  class TestFragment(output: Fragment[Foo]) extends Fragment[Foo] {

    override def doAdd(foo: Foo): Unit = {
      foo.str.modify(s"${BatchContext.get("batcharg")}_${foo.id.get}")
      output.add(foo)
    }

    override def doReset(): Unit = {
      output.reset()
    }
  }
}
