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
package com.asakusafw.spark.compiler
package operator
package user

import org.junit.runner.RunWith
import org.scalatest.FlatSpec
import org.scalatest.junit.JUnitRunner

import java.io.{ DataInput, DataOutput }
import java.util.{ List => JList }

import scala.collection.JavaConversions._

import org.apache.hadoop.io.Writable
import org.apache.spark.broadcast.Broadcast

import com.asakusafw.lang.compiler.model.description.{ ClassDescription, ImmediateDescription }
import com.asakusafw.lang.compiler.model.graph.Groups
import com.asakusafw.lang.compiler.model.testing.OperatorExtractor
import com.asakusafw.runtime.core.Result
import com.asakusafw.runtime.model.DataModel
import com.asakusafw.runtime.value.{ IntOption, StringOption }
import com.asakusafw.spark.compiler.spi.{ OperatorCompiler, OperatorType }
import com.asakusafw.spark.runtime.fragment.{ Fragment, GenericOutputFragment }
import com.asakusafw.spark.runtime.graph.BroadcastId
import com.asakusafw.spark.tools.asm._
import com.asakusafw.vocabulary.operator.CoGroup

@RunWith(classOf[JUnitRunner])
class CoGroupOperatorCompilerSpecTest extends CoGroupOperatorCompilerSpec

class CoGroupOperatorCompilerSpec extends FlatSpec with UsingCompilerContext {

  import CoGroupOperatorCompilerSpec._

  behavior of classOf[CoGroupOperatorCompiler].getSimpleName

  for {
    s <- Seq("s", null)
  } {
    it should s"compile CoGroup operator${if (s == null) " with argument null" else ""}" in {
      val operator = OperatorExtractor
        .extract(classOf[CoGroup], classOf[CoGroupOperator], "cogroup")
        .input("foos", ClassDescription.of(classOf[Foo]),
          Groups.parse(Seq("id")))
        .input("bars", ClassDescription.of(classOf[Bar]),
          Groups.parse(Seq("fooId"), Seq("+id")))
        .output("fooResult", ClassDescription.of(classOf[Foo]))
        .output("barResult", ClassDescription.of(classOf[Bar]))
        .output("fooError", ClassDescription.of(classOf[Foo]))
        .output("barError", ClassDescription.of(classOf[Bar]))
        .output("nResult", ClassDescription.of(classOf[N]))
        .argument("n", ImmediateDescription.of(10))
        .argument("s", ImmediateDescription.of(s))
        .build()

      implicit val context = newOperatorCompilerContext("flowId")

      val thisType = OperatorCompiler.compile(operator, OperatorType.CoGroupType)
      val cls = context.loadClass[Fragment[IndexedSeq[Iterator[_]]]](thisType.getClassName)

      val fooResult = new GenericOutputFragment[Foo]()
      val fooError = new GenericOutputFragment[Foo]()

      val barResult = new GenericOutputFragment[Bar]()
      val barError = new GenericOutputFragment[Bar]()

      val nResult = new GenericOutputFragment[N]()

      val fragment = cls.getConstructor(
        classOf[Map[BroadcastId, Broadcast[_]]],
        classOf[Fragment[_]], classOf[Fragment[_]],
        classOf[Fragment[_]], classOf[Fragment[_]],
        classOf[Fragment[_]])
        .newInstance(Map.empty, fooResult, barResult, fooError, barError, nResult)

      {
        fragment.reset()
        val foos = Seq.empty[Foo]
        val bars = Seq.empty[Bar]
        fragment.add(IndexedSeq(foos.iterator, bars.iterator))
        assert(fooResult.iterator.size === 0)
        assert(barResult.iterator.size === 0)
        assert(fooError.iterator.size === 0)
        assert(barError.iterator.size === 0)
        val nResults = nResult.iterator.toSeq
        assert(nResults.size === 1)
        assert(nResults.head.n.get === 10)
      }

      fragment.reset()
      assert(fooResult.iterator.size === 0)
      assert(barResult.iterator.size === 0)
      assert(fooError.iterator.size === 0)
      assert(barError.iterator.size === 0)
      assert(nResult.iterator.size === 0)

      {
        fragment.reset()
        val foo = new Foo()
        foo.id.modify(1)
        val foos = Seq(foo)
        val bar = new Bar()
        bar.id.modify(10)
        bar.fooId.modify(1)
        val bars = Seq(bar)
        fragment.add(IndexedSeq(foos.iterator, bars.iterator))
        val fooResults = fooResult.iterator.toSeq
        assert(fooResults.size === 1)
        assert(fooResults.head.id.get === foo.id.get)
        if (s == null) {
          assert(fooResults.head.s.isNull)
        } else {
          assert(fooResults.head.s.getAsString === s)
        }
        val barResults = barResult.iterator.toSeq
        assert(barResults.size === 1)
        assert(barResults.head.id.get === bar.id.get)
        assert(barResults.head.fooId.get === bar.fooId.get)
        if (s == null) {
          assert(barResults.head.s.isNull)
        } else {
          assert(barResults.head.s.getAsString === s)
        }
        val fooErrors = fooError.iterator.toSeq
        assert(fooErrors.size === 0)
        val barErrors = barError.iterator.toSeq
        assert(barErrors.size === 0)
        val nResults = nResult.iterator.toSeq
        assert(nResults.size === 1)
        assert(nResults.head.n.get === 10)
      }

      fragment.reset()
      assert(fooResult.iterator.size === 0)
      assert(barResult.iterator.size === 0)
      assert(fooError.iterator.size === 0)
      assert(barError.iterator.size === 0)
      assert(nResult.iterator.size === 0)

      {
        fragment.reset()
        val foo = new Foo()
        foo.id.modify(1)
        val foos = Seq(foo)
        val bars = (0 until 10).map { i =>
          val bar = new Bar()
          bar.id.modify(i)
          bar.fooId.modify(1)
          bar
        }
        fragment.add(IndexedSeq(foos.iterator, bars.iterator))
        val fooResults = fooResult.iterator.toSeq
        assert(fooResults.size === 0)
        val barResults = barResult.iterator.toSeq
        assert(barResults.size === 0)
        val fooErrors = fooError.iterator.toSeq
        assert(fooErrors.size === 1)
        assert(fooErrors.head.id.get === foo.id.get)
        if (s == null) {
          assert(fooErrors.head.s.isNull)
        } else {
          assert(fooErrors.head.s.getAsString === s)
        }
        val barErrors = barError.iterator.toSeq
        assert(barErrors.size === 10)
        barErrors.zip(bars).foreach {
          case (actual, expected) =>
            assert(actual.id.get === expected.id.get)
            assert(actual.fooId.get === expected.fooId.get)
            if (s == null) {
              assert(actual.s.isNull)
            } else {
              assert(actual.s.getAsString === s)
            }
        }
        val nResults = nResult.iterator.toSeq
        assert(nResults.size === 1)
        assert(nResults.head.n.get === 10)
      }

      fragment.reset()
      assert(fooResult.iterator.size === 0)
      assert(barResult.iterator.size === 0)
      assert(fooError.iterator.size === 0)
      assert(barError.iterator.size === 0)
      assert(nResult.iterator.size === 0)
    }
  }
}

object CoGroupOperatorCompilerSpec {

  trait FooP {
    def getIdOption: IntOption
  }

  class Foo extends DataModel[Foo] with FooP with Writable {

    val id = new IntOption()
    val s = new StringOption()

    override def reset(): Unit = {
      id.setNull()
      s.setNull()
    }
    override def copyFrom(other: Foo): Unit = {
      id.copyFrom(other.id)
      s.copyFrom(other.s)
    }
    override def readFields(in: DataInput): Unit = {
      id.readFields(in)
      s.readFields(in)
    }
    override def write(out: DataOutput): Unit = {
      id.write(out)
      s.write(out)
    }

    def getIdOption: IntOption = id
  }

  trait BarP {
    def getIdOption: IntOption
    def getFooIdOption: IntOption
  }

  class Bar extends DataModel[Bar] with BarP with Writable {

    val id = new IntOption()
    val fooId = new IntOption()
    val s = new StringOption()

    override def reset(): Unit = {
      id.setNull()
      fooId.setNull()
      s.setNull()
    }
    override def copyFrom(other: Bar): Unit = {
      id.copyFrom(other.id)
      fooId.copyFrom(other.fooId)
      s.copyFrom(other.s)
    }
    override def readFields(in: DataInput): Unit = {
      id.readFields(in)
      fooId.readFields(in)
      s.readFields(in)
    }
    override def write(out: DataOutput): Unit = {
      id.write(out)
      fooId.write(out)
      s.write(out)
    }

    def getIdOption: IntOption = id
    def getFooIdOption: IntOption = fooId
  }

  class N extends DataModel[N] with Writable {

    val n = new IntOption()

    override def reset(): Unit = {
      n.setNull()
    }
    override def copyFrom(other: N): Unit = {
      n.copyFrom(other.n)
    }
    override def readFields(in: DataInput): Unit = {
      n.readFields(in)
    }
    override def write(out: DataOutput): Unit = {
      n.write(out)
    }

    def getIOption: IntOption = n
  }

  class CoGroupOperator {

    private[this] val n = new N

    @CoGroup
    def cogroup(
      foos: JList[Foo], bars: JList[Bar],
      fooResult: Result[Foo], barResult: Result[Bar],
      fooError: Result[Foo], barError: Result[Bar],
      nResult: Result[N],
      n: Int,
      s: String): Unit = {
      if (s != null) {
        foos.foreach(_.s.modify(s))
        bars.foreach(_.s.modify(s))
      }
      if (foos.size == 1 && bars.size == 1) {
        fooResult.add(foos(0))
        barResult.add(bars(0))
      } else {
        foos.foreach(fooError.add)
        bars.foreach(barError.add)
      }
      this.n.n.modify(n)
      nResult.add(this.n)
    }

    @CoGroup
    def cogroupp[F <: FooP, B <: BarP](
      foos: JList[F], bars: JList[B],
      fooResult: Result[F], barResult: Result[B],
      fooError: Result[F], barError: Result[B],
      nResult: Result[N],
      n: Int): Unit = {
      if (foos.size == 1 && bars.size == 1) {
        fooResult.add(foos(0))
        barResult.add(bars(0))
      } else {
        foos.foreach(fooError.add)
        bars.foreach(barError.add)
      }
      this.n.n.modify(n)
      nResult.add(this.n)
    }
  }
}
