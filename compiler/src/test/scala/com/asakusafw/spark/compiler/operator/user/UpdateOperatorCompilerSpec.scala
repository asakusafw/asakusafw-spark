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
import com.asakusafw.runtime.value.{ IntOption, LongOption, StringOption }
import com.asakusafw.spark.compiler.broadcast.MockBroadcast
import com.asakusafw.spark.compiler.spi.{ OperatorCompiler, OperatorType }
import com.asakusafw.spark.runtime.fragment.{ Fragment, GenericOutputFragment }
import com.asakusafw.spark.runtime.graph.BroadcastId
import com.asakusafw.spark.runtime.io.WritableSerDe
import com.asakusafw.spark.runtime.rdd.ShuffleKey
import com.asakusafw.spark.tools.asm._
import com.asakusafw.vocabulary.operator.Update

@RunWith(classOf[JUnitRunner])
class UpdateOperatorCompilerSpecTest extends UpdateOperatorCompilerSpec

class UpdateOperatorCompilerSpec extends FlatSpec with UsingCompilerContext {

  import UpdateOperatorCompilerSpec._

  behavior of classOf[UpdateOperatorCompiler].getSimpleName

  for {
    s <- Seq("s", null)
  } {
    it should s"compile Update operator${if (s == null) " with argument null" else ""}" in {
      val operator = OperatorExtractor.extract(
        classOf[Update], classOf[UpdateOperator], "update")
        .input("in", ClassDescription.of(classOf[Foo]))
        .output("out", ClassDescription.of(classOf[Foo]))
        .argument("rate", ImmediateDescription.of(100))
        .argument("s", ImmediateDescription.of(s))
        .build();

      implicit val context = newOperatorCompilerContext("flowId")

      val thisType = OperatorCompiler.compile(operator, OperatorType.ExtractType)
      val cls = context.loadClass[Fragment[Foo]](thisType.getClassName)

      val out = new GenericOutputFragment[Foo]()

      val fragment = cls
        .getConstructor(classOf[Map[BroadcastId, Broadcasted[_]]], classOf[Fragment[_]])
        .newInstance(Map.empty, out)

      fragment.reset()
      val foo = new Foo()
      for (i <- 0 until 10) {
        foo.reset()
        foo.i.modify(i)
        fragment.add(foo)
      }
      out.iterator.zipWithIndex.foreach {
        case (foo, i) =>
          assert(foo.i.get === i)
          assert(foo.l.get === i * 100)
          if (s == null) {
            assert(foo.s.isNull)
          } else {
            assert(foo.s.getAsString === s)
          }
      }

      fragment.reset()
    }
  }

  it should "compile Update operator with projective model" in {
    val operator = OperatorExtractor.extract(
      classOf[Update], classOf[UpdateOperator], "updatep")
      .input("in", ClassDescription.of(classOf[Foo]))
      .output("out", ClassDescription.of(classOf[Foo]))
      .argument("rate", ImmediateDescription.of(100))
      .build();

    implicit val context = newOperatorCompilerContext("flowId")

    val thisType = OperatorCompiler.compile(operator, OperatorType.ExtractType)
    val cls = context.loadClass[Fragment[Foo]](thisType.getClassName)

    val out = new GenericOutputFragment[Foo]()

    val fragment = cls
      .getConstructor(classOf[Map[BroadcastId, Broadcasted[_]]], classOf[Fragment[_]])
      .newInstance(Map.empty, out)

    fragment.reset()
    val foo = new Foo()
    for (i <- 0 until 10) {
      foo.reset()
      foo.i.modify(i)
      fragment.add(foo)
    }
    out.iterator.zipWithIndex.foreach {
      case (foo, i) =>
        assert(foo.i.get === i)
        assert(foo.l.get === i * 100)
        assert(foo.s.isNull)
    }

    fragment.reset()
  }

  it should "compile Update operator with view" in {
    val vMarker = MarkerOperator.builder(ClassDescription.of(classOf[Foo]))
      .attribute(classOf[PlanMarker], PlanMarker.BROADCAST).build()
    val gvMarker = MarkerOperator.builder(ClassDescription.of(classOf[Foo]))
      .attribute(classOf[PlanMarker], PlanMarker.BROADCAST).build()

    val operator = OperatorExtractor.extract(
      classOf[Update], classOf[UpdateOperator], "updateWithView")
      .input("in", ClassDescription.of(classOf[Foo]))
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
      .output("out", ClassDescription.of(classOf[Foo]))
      .argument("rate", ImmediateDescription.of(100))
      .build();

    implicit val context = newOperatorCompilerContext("flowId")

    val thisType = OperatorCompiler.compile(operator, OperatorType.ExtractType)
    context.addClass(context.broadcastIds)
    val cls = context.loadClass[Fragment[Foo]](thisType.getClassName)

    val broadcastIdsCls = context.loadClass(context.broadcastIds.thisType.getClassName)
    def getBroadcastId(marker: MarkerOperator): BroadcastId = {
      val sn = marker.getSerialNumber
      broadcastIdsCls.getField(context.broadcastIds.getField(sn)).get(null).asInstanceOf[BroadcastId]
    }

    val out = new GenericOutputFragment[Foo]()

    val view = new MockBroadcast(0, Map(ShuffleKey.empty -> Seq(new Foo())))
    val groupview = new MockBroadcast(1,
      (0 until 10).map { i =>
        val foo = new Foo()
        foo.i.modify(i)
        new ShuffleKey(WritableSerDe.serialize(foo.i)) -> Seq(foo)
      }.toMap)

    val fragment = cls
      .getConstructor(classOf[Map[BroadcastId, Broadcasted[_]]], classOf[Fragment[_]])
      .newInstance(
        Map(
          getBroadcastId(vMarker) -> view,
          getBroadcastId(gvMarker) -> groupview),
        out)

    fragment.reset()
    val foo = new Foo()
    for (i <- 0 until 10) {
      foo.reset()
      foo.i.modify(i)
      fragment.add(foo)
    }
    out.iterator.zipWithIndex.foreach {
      case (foo, i) =>
        assert(foo.i.get === i)
        assert(foo.l.get === i * 100)
        assert(foo.s.isNull)
    }

    fragment.reset()
  }
}

object UpdateOperatorCompilerSpec {

  trait FooP {
    def getIOption: IntOption
    def getLOption: LongOption
  }

  class Foo extends DataModel[Foo] with FooP with Writable {

    val i: IntOption = new IntOption()
    val l: LongOption = new LongOption()
    val s: StringOption = new StringOption()

    override def reset: Unit = {
      i.setNull()
      l.setNull()
      s.setNull()
    }
    override def copyFrom(other: Foo): Unit = {
      i.copyFrom(other.i)
      l.copyFrom(other.l)
      s.copyFrom(other.s)
    }
    override def readFields(in: DataInput): Unit = {
      i.readFields(in)
      l.readFields(in)
      s.readFields(in)
    }
    override def write(out: DataOutput): Unit = {
      i.write(out)
      l.write(out)
      s.write(out)
    }

    def getIOption: IntOption = i
    def getLOption: LongOption = l
  }

  class UpdateOperator {

    @Update
    def update(foo: Foo, rate: Int, s: String): Unit = {
      foo.l.modify(rate * foo.i.get)
      if (s != null) {
        foo.s.modify(s)
      }
    }

    @Update
    def updatep[F <: FooP](foo: F, rate: Int): Unit = {
      foo.getLOption.modify(rate * foo.getIOption.get)
    }

    @Update
    def updateWithView(foo: Foo, v: View[Foo], gv: GroupView[Foo], rate: Int): Unit = {
      foo.l.modify(rate * foo.i.get)
    }
  }
}
