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
import com.asakusafw.runtime.core.{ GroupView, Result, View }
import com.asakusafw.runtime.model.DataModel
import com.asakusafw.runtime.value.{ IntOption, LongOption, StringOption }
import com.asakusafw.spark.compiler.broadcast.MockBroadcast
import com.asakusafw.spark.compiler.spi.{ OperatorCompiler, OperatorType }
import com.asakusafw.spark.runtime.fragment.{ Fragment, GenericOutputFragment }
import com.asakusafw.spark.runtime.graph.BroadcastId
import com.asakusafw.spark.runtime.io.WritableSerDe
import com.asakusafw.spark.runtime.rdd.ShuffleKey
import com.asakusafw.spark.tools.asm._
import com.asakusafw.vocabulary.operator.Extract

@RunWith(classOf[JUnitRunner])
class ExtractOperatorCompilerSpecTest extends ExtractOperatorCompilerSpec

class ExtractOperatorCompilerSpec extends FlatSpec with UsingCompilerContext {

  import ExtractOperatorCompilerSpec._

  behavior of classOf[ExtractOperatorCompiler].getSimpleName

  for {
    s <- Seq("s", null)
  } {
    it should s"compile Extract operator${if (s == null) " with argument null" else ""}" in {
      val operator = OperatorExtractor
        .extract(classOf[Extract], classOf[ExtractOperator], "extract")
        .input("input", ClassDescription.of(classOf[Input]))
        .output("output1", ClassDescription.of(classOf[IntOutput]))
        .output("output2", ClassDescription.of(classOf[LongOutput]))
        .argument("n", ImmediateDescription.of(10))
        .argument("s", ImmediateDescription.of(s))
        .build()

      implicit val context = newOperatorCompilerContext("flowId")

      val thisType = OperatorCompiler.compile(operator, OperatorType.ExtractType)
      val cls = context.loadClass[Fragment[Input]](thisType.getClassName)

      val out1 = new GenericOutputFragment[IntOutput]()
      val out2 = new GenericOutputFragment[LongOutput]()

      val fragment = cls
        .getConstructor(
          classOf[Map[BroadcastId, Broadcasted[_]]],
          classOf[Fragment[_]], classOf[Fragment[_]])
        .newInstance(Map.empty, out1, out2)

      fragment.reset()
      val input = new Input()
      for (i <- 0 until 10) {
        input.i.modify(i)
        input.l.modify(i)
        fragment.add(input)
      }
      out1.iterator.zipWithIndex.foreach {
        case (output, i) =>
          assert(output.i.get === i)
          if (s == null) {
            assert(output.s.isNull)
          } else {
            assert(output.s.getAsString === s)
          }
      }
      out2.iterator.zipWithIndex.foreach {
        case (output, i) =>
          assert(output.l.get === i / 10)
          if (s == null) {
            assert(output.s.isNull)
          } else {
            assert(output.s.getAsString === s)
          }
      }

      fragment.reset()
    }
  }

  it should "compile Extract operator with projective model" in {
    val operator = OperatorExtractor
      .extract(classOf[Extract], classOf[ExtractOperator], "extractp")
      .input("input", ClassDescription.of(classOf[Input]))
      .output("output1", ClassDescription.of(classOf[IntOutput]))
      .output("output2", ClassDescription.of(classOf[LongOutput]))
      .argument("n", ImmediateDescription.of(10))
      .build()

    implicit val context = newOperatorCompilerContext("flowId")

    val thisType = OperatorCompiler.compile(operator, OperatorType.ExtractType)
    val cls = context.loadClass[Fragment[Input]](thisType.getClassName)

    val out1 = new GenericOutputFragment[IntOutput]()
    val out2 = new GenericOutputFragment[LongOutput]()

    val fragment = cls
      .getConstructor(
        classOf[Map[BroadcastId, Broadcasted[_]]],
        classOf[Fragment[_]], classOf[Fragment[_]])
      .newInstance(Map.empty, out1, out2)

    fragment.reset()
    val input = new Input()
    for (i <- 0 until 10) {
      input.i.modify(i)
      input.l.modify(i)
      fragment.add(input)
    }
    out1.iterator.zipWithIndex.foreach {
      case (output, i) =>
        assert(output.i.get === i)
        assert(output.s.isNull)
    }
    out2.iterator.zipWithIndex.foreach {
      case (output, i) =>
        assert(output.l.get === i / 10)
        assert(output.s.isNull)
    }

    fragment.reset()
  }

  it should "compile Extract operator with view" in {
    val vMarker = MarkerOperator.builder(ClassDescription.of(classOf[Input]))
      .attribute(classOf[PlanMarker], PlanMarker.BROADCAST).build()
    val gvMarker = MarkerOperator.builder(ClassDescription.of(classOf[Input]))
      .attribute(classOf[PlanMarker], PlanMarker.BROADCAST).build()

    val operator = OperatorExtractor
      .extract(classOf[Extract], classOf[ExtractOperator], "extractWithView")
      .input("input", ClassDescription.of(classOf[Input]))
      .input("v", ClassDescription.of(classOf[Input]),
        new Consumer[Operator.InputOptionBuilder] {
          override def accept(builder: Operator.InputOptionBuilder): Unit = {
            builder
              .unit(OperatorInput.InputUnit.WHOLE)
              .group(Groups.parse(Seq.empty, Seq.empty))
              .upstream(vMarker.getOutput)
          }
        })
      .input("gv", ClassDescription.of(classOf[Input]),
        new Consumer[Operator.InputOptionBuilder] {
          override def accept(builder: Operator.InputOptionBuilder): Unit = {
            builder
              .unit(OperatorInput.InputUnit.WHOLE)
              .group(Groups.parse(Seq("i"), Seq.empty))
              .upstream(gvMarker.getOutput)
          }
        })
      .output("output1", ClassDescription.of(classOf[IntOutput]))
      .output("output2", ClassDescription.of(classOf[LongOutput]))
      .argument("n", ImmediateDescription.of(10))
      .build()

    implicit val context = newOperatorCompilerContext("flowId")

    val thisType = OperatorCompiler.compile(operator, OperatorType.ExtractType)
    context.addClass(context.broadcastIds)
    val cls = context.loadClass[Fragment[Input]](thisType.getClassName)

    val broadcastIdsCls = context.loadClass(context.broadcastIds.thisType.getClassName)
    def getBroadcastId(marker: MarkerOperator): BroadcastId = {
      val sn = marker.getSerialNumber
      broadcastIdsCls.getField(context.broadcastIds.getField(sn)).get(null).asInstanceOf[BroadcastId]
    }

    val out1 = new GenericOutputFragment[IntOutput]()
    val out2 = new GenericOutputFragment[LongOutput]()

    val view = new MockBroadcast(0, Map(ShuffleKey.empty -> Seq(new Input())))
    val groupview = new MockBroadcast(1,
      (0 until 10).map { i =>
        val input = new Input()
        input.i.modify(i)
        new ShuffleKey(WritableSerDe.serialize(input.i)) -> Seq(input)
      }.toMap)

    val fragment = cls
      .getConstructor(
        classOf[Map[BroadcastId, Broadcasted[_]]],
        classOf[Fragment[_]], classOf[Fragment[_]])
      .newInstance(Map(
        getBroadcastId(vMarker) -> view,
        getBroadcastId(gvMarker) -> groupview),
        out1, out2)

    fragment.reset()
    val input = new Input()
    for (i <- 0 until 10) {
      input.i.modify(i)
      input.l.modify(i)
      fragment.add(input)
    }
    out1.iterator.zipWithIndex.foreach {
      case (output, i) =>
        assert(output.i.get === i)
        assert(output.s.isNull)
    }
    out2.iterator.zipWithIndex.foreach {
      case (output, i) =>
        assert(output.l.get === i / 10)
        assert(output.s.isNull)
    }

    fragment.reset()
  }
}

object ExtractOperatorCompilerSpec {

  trait InputP {
    def getIOption: IntOption
    def getLOption: LongOption
  }

  class Input extends DataModel[Input] with InputP with Writable {

    val i: IntOption = new IntOption()
    val l: LongOption = new LongOption()

    override def reset: Unit = {
      i.setNull()
      l.setNull()
    }
    override def copyFrom(other: Input): Unit = {
      i.copyFrom(other.i)
      l.copyFrom(other.l)
    }
    override def readFields(in: DataInput): Unit = {
      i.readFields(in)
      l.readFields(in)
    }
    override def write(out: DataOutput): Unit = {
      i.write(out)
      l.write(out)
    }

    def getIOption: IntOption = i
    def getLOption: LongOption = l
  }

  trait IntOutputP {
    def getIOption: IntOption
  }

  class IntOutput extends DataModel[IntOutput] with IntOutputP with Writable {

    val i: IntOption = new IntOption()
    val s: StringOption = new StringOption()

    override def reset: Unit = {
      i.setNull()
      s.setNull()
    }
    override def copyFrom(other: IntOutput): Unit = {
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

  trait LongOutputP {
    def getLOption: LongOption
  }

  class LongOutput extends DataModel[LongOutput] with LongOutputP with Writable {

    val l: LongOption = new LongOption()
    val s: StringOption = new StringOption()

    override def reset: Unit = {
      l.setNull()
      s.setNull()
    }
    override def copyFrom(other: LongOutput): Unit = {
      l.copyFrom(other.l)
      s.copyFrom(other.s)
    }
    override def readFields(in: DataInput): Unit = {
      l.readFields(in)
      s.readFields(in)
    }
    override def write(out: DataOutput): Unit = {
      l.write(out)
      s.write(out)
    }

    def getLOption: LongOption = l
  }

  class ExtractOperator {

    private[this] val i = new IntOutput()
    private[this] val l = new LongOutput()

    @Extract
    def extract(
      in: Input,
      out1: Result[IntOutput],
      out2: Result[LongOutput],
      n: Int,
      s: String): Unit = {
      i.i.copyFrom(in.getIOption)
      if (s != null) {
        i.s.modify(s)
      }
      out1.add(i)
      for (_ <- 0 until n) {
        l.l.copyFrom(in.getLOption)
        if (s != null) {
          l.s.modify(s)
        }
        out2.add(l)
      }
    }

    @Extract
    def extractp[I <: InputP, IO <: IntOutputP, LO <: LongOutputP](
      in: I,
      out1: Result[IO],
      out2: Result[LO],
      n: Int): Unit = {
      i.getIOption.copyFrom(in.getIOption)
      out1.add(i.asInstanceOf[IO])
      for (_ <- 0 until n) {
        l.getLOption.copyFrom(in.getLOption)
        out2.add(l.asInstanceOf[LO])
      }
    }

    @Extract
    def extractWithView(
      in: Input,
      v: View[Input],
      gv: GroupView[Input],
      out1: Result[IntOutput],
      out2: Result[LongOutput],
      n: Int): Unit = {
      i.i.copyFrom(in.getIOption)
      out1.add(i)
      for (_ <- 0 until n) {
        l.l.copyFrom(in.getLOption)
        out2.add(l)
      }
    }
  }
}
