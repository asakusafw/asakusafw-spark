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
package com.asakusafw.spark.compiler
package operator
package user

import org.junit.runner.RunWith
import org.scalatest.FlatSpec
import org.scalatest.junit.JUnitRunner

import java.io.{ DataInput, DataOutput }
import java.util.function.Consumer

import scala.collection.JavaConversions._

import org.apache.hadoop.io.Writable
import org.apache.spark.broadcast.{ Broadcast => Broadcasted }

import com.asakusafw.lang.compiler.model.description.{ ClassDescription, ImmediateDescription }
import com.asakusafw.lang.compiler.model.graph.{ Groups, MarkerOperator, Operator, OperatorInput }
import com.asakusafw.lang.compiler.model.testing.OperatorExtractor
import com.asakusafw.lang.compiler.planning.PlanMarker
import com.asakusafw.runtime.core.{ GroupView, View }
import com.asakusafw.runtime.model.DataModel
import com.asakusafw.runtime.value.{ IntOption, StringOption }
import com.asakusafw.spark.compiler.broadcast.MockBroadcast
import com.asakusafw.spark.compiler.spi.{ OperatorCompiler, OperatorType }
import com.asakusafw.spark.runtime.fragment.{ Fragment, GenericOutputFragment }
import com.asakusafw.spark.runtime.graph.BroadcastId
import com.asakusafw.spark.runtime.io.WritableSerDe
import com.asakusafw.spark.runtime.rdd.ShuffleKey
import com.asakusafw.spark.tools.asm._
import com.asakusafw.vocabulary.operator.Branch

@RunWith(classOf[JUnitRunner])
class BranchOperatorCompilerSpecTest extends BranchOperatorCompilerSpec

class BranchOperatorCompilerSpec extends FlatSpec with UsingCompilerContext {

  import BranchOperatorCompilerSpec._

  behavior of classOf[BranchOperatorCompiler].getSimpleName

  for {
    s <- Seq("s", null)
  } {
    it should s"compile Branch operator${if (s == null) " with argument null" else ""}" in {
      val operator = OperatorExtractor
        .extract(classOf[Branch], classOf[BranchOperator], "branch")
        .input("input", ClassDescription.of(classOf[Foo]))
        .output("low", ClassDescription.of(classOf[Foo]))
        .output("high", ClassDescription.of(classOf[Foo]))
        .argument("n", ImmediateDescription.of(10))
        .argument("s", ImmediateDescription.of(s))
        .build()

      implicit val context = newOperatorCompilerContext("flowId")

      val thisType = OperatorCompiler.compile(operator, OperatorType.ExtractType)
      val cls = context.loadClass[Fragment[Foo]](thisType.getClassName)

      val out1 = new GenericOutputFragment[Foo]()
      val out2 = new GenericOutputFragment[Foo]()

      val fragment = cls
        .getConstructor(
          classOf[Map[BroadcastId, Broadcasted[_]]],
          classOf[Fragment[_]], classOf[Fragment[_]])
        .newInstance(Map.empty, out1, out2)

      val foo = new Foo()
      for (i <- 0 until 10) {
        foo.i.modify(i)
        fragment.add(foo)
      }
      val result1 = out1.iterator.toSeq
      assert(result1.size === (if (s == null) 0 else 5))
      result1.zipWithIndex.foreach {
        case (foo, i) =>
          assert(foo.i.get === i)
      }
      val result2 = out2.iterator.toSeq
      assert(result2.size == (if (s == null) 10 else 5))
      result2.zipWithIndex.foreach {
        case (foo, i) =>
          assert(foo.i.get === i + (if (s == null) 0 else 5))
      }
      fragment.reset()
    }
  }

  it should "compile Branch operator with projective model" in {
    val operator = OperatorExtractor
      .extract(classOf[Branch], classOf[BranchOperator], "branchp")
      .input("input", ClassDescription.of(classOf[Foo]))
      .output("low", ClassDescription.of(classOf[Foo]))
      .output("high", ClassDescription.of(classOf[Foo]))
      .argument("n", ImmediateDescription.of(10))
      .build()

    implicit val context = newOperatorCompilerContext("flowId")

    val thisType = OperatorCompiler.compile(operator, OperatorType.ExtractType)
    val cls = context.loadClass[Fragment[Foo]](thisType.getClassName)

    val out1 = new GenericOutputFragment[Foo]()
    val out2 = new GenericOutputFragment[Foo]()

    val fragment = cls
      .getConstructor(
        classOf[Map[BroadcastId, Broadcasted[_]]],
        classOf[Fragment[_]], classOf[Fragment[_]])
      .newInstance(Map.empty, out1, out2)

    fragment.reset()
    val foo = new Foo()
    for (i <- 0 until 10) {
      foo.i.modify(i)
      fragment.add(foo)
    }
    out1.iterator.zipWithIndex.foreach {
      case (foo, i) =>
        assert(foo.i.get === i)
        assert(foo.s.isNull)
    }
    out2.iterator.zipWithIndex.foreach {
      case (foo, i) =>
        assert(foo.i.get === i + 5)
        assert(foo.s.isNull)
    }

    fragment.reset()
  }

  it should "compile Branch operator with view" in {
    val vMarker = MarkerOperator.builder(ClassDescription.of(classOf[Foo]))
      .attribute(classOf[PlanMarker], PlanMarker.BROADCAST).build()
    val gvMarker = MarkerOperator.builder(ClassDescription.of(classOf[Foo]))
      .attribute(classOf[PlanMarker], PlanMarker.BROADCAST).build()

    val operator = OperatorExtractor
      .extract(classOf[Branch], classOf[BranchOperator], "branchWithView")
      .input("input", ClassDescription.of(classOf[Foo]))
      .input("v", ClassDescription.of(classOf[Foo]),
        new Consumer[Operator.InputOptionBuilder] {
          override def accept(builder: Operator.InputOptionBuilder): Unit = {
            builder
              .unit(OperatorInput.InputUnit.WHOLE)
              .group(Groups.parse(Seq.empty, Seq.empty))
              .upstream(vMarker.getOutput)
          }
        })
      .input("gv", ClassDescription.of(classOf[Foo]),
        new Consumer[Operator.InputOptionBuilder] {
          override def accept(builder: Operator.InputOptionBuilder): Unit = {
            builder
              .unit(OperatorInput.InputUnit.WHOLE)
              .group(Groups.parse(Seq("i"), Seq.empty))
              .upstream(gvMarker.getOutput)
          }
        })
      .output("low", ClassDescription.of(classOf[Foo]))
      .output("high", ClassDescription.of(classOf[Foo]))
      .build()

    implicit val context = newOperatorCompilerContext("flowId")

    val thisType = OperatorCompiler.compile(operator, OperatorType.ExtractType)
    context.addClass(context.broadcastIds)
    val cls = context.loadClass[Fragment[Foo]](thisType.getClassName)

    val broadcastIdsCls = context.loadClass(context.broadcastIds.thisType.getClassName)
    def getBroadcastId(marker: MarkerOperator): BroadcastId = {
      val sn = marker.getSerialNumber
      broadcastIdsCls.getField(context.broadcastIds.getField(sn)).get(null).asInstanceOf[BroadcastId]
    }

    val out1 = new GenericOutputFragment[Foo]()
    val out2 = new GenericOutputFragment[Foo]()

    val view = new MockBroadcast(0, Map(ShuffleKey.empty -> Seq(new Foo())))
    val groupview = new MockBroadcast(1,
      (0 until 10).map { i =>
        val foo = new Foo()
        foo.i.modify(i)
        new ShuffleKey(WritableSerDe.serialize(foo.i)) -> Seq(foo)
      }.toMap)

    val fragment = cls
      .getConstructor(
        classOf[Map[BroadcastId, Broadcasted[_]]],
        classOf[Fragment[_]], classOf[Fragment[_]])
      .newInstance(
        Map(
          getBroadcastId(vMarker) -> view,
          getBroadcastId(gvMarker) -> groupview),
        out1, out2)

    fragment.reset()
    val foo = new Foo()
    for (i <- 0 until 10) {
      foo.i.modify(i)
      fragment.add(foo)
    }
    out1.iterator.zipWithIndex.foreach {
      case (foo, i) =>
        assert(foo.i.get === i)
        assert(foo.s.isNull)
    }
    out2.iterator.zipWithIndex.foreach {
      case (foo, i) =>
        assert(foo.i.get === i + 5)
        assert(foo.s.isNull)
    }

    fragment.reset()
  }
}

object BranchOperatorCompilerSpec {

  trait FooP {
    def getIOption: IntOption
  }

  class Foo extends DataModel[Foo] with FooP with Writable {

    val i: IntOption = new IntOption()
    val s: StringOption = new StringOption()

    override def reset: Unit = {
      i.setNull()
      s.setNull()
    }
    override def copyFrom(other: Foo): Unit = {
      i.copyFrom(other.i)
      s.copyFrom(other.s)
    }
    override def readFields(in: DataInput): Unit = {
      i.readFields(in)
      s.readFields(in)
    }
    override def write(out: DataOutput): Unit = {
      i.write(out)
      s.write(out)
    }

    def getIOption: IntOption = i
  }

  class BranchOperator {

    @Branch
    def branch(in: Foo, n: Int, s: String): BranchOperatorCompilerSpecTestBranch = {
      if (in.i.get < 5 && s != null) {
        BranchOperatorCompilerSpecTestBranch.LOW
      } else {
        BranchOperatorCompilerSpecTestBranch.HIGH
      }
    }

    @Branch
    def branchp[F <: FooP](in: F, n: Int): BranchOperatorCompilerSpecTestBranch = {
      if (in.getIOption.get < 5) {
        BranchOperatorCompilerSpecTestBranch.LOW
      } else {
        BranchOperatorCompilerSpecTestBranch.HIGH
      }
    }

    @Branch
    def branchWithView(in: Foo, v: View[Foo], gv: GroupView[Foo]): BranchOperatorCompilerSpecTestBranch = {
      if (in.i.get < 5) {
        BranchOperatorCompilerSpecTestBranch.LOW
      } else {
        BranchOperatorCompilerSpecTestBranch.HIGH
      }
    }
  }
}
