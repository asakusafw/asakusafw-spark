/*
 * Copyright 2011-2017 Asakusa Framework Team.
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
import scala.language.reflectiveCalls

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
import com.asakusafw.vocabulary.operator.Convert

@RunWith(classOf[JUnitRunner])
class ConvertOperatorCompilerSpecTest extends ConvertOperatorCompilerSpec

class ConvertOperatorCompilerSpec extends FlatSpec with UsingCompilerContext {

  import ConvertOperatorCompilerSpec._

  behavior of classOf[ConvertOperatorCompiler].getSimpleName

  for {
    s <- Seq("s", null)
  } {
    it should s"compile Convert operator${if (s == null) " with argument null" else ""}" in {
      val operator = OperatorExtractor
        .extract(classOf[Convert], classOf[ConvertOperator], "convert")
        .input("input", ClassDescription.of(classOf[Input]))
        .output("original", ClassDescription.of(classOf[Input]))
        .output("out", ClassDescription.of(classOf[Output]))
        .argument("n", ImmediateDescription.of(10))
        .argument("s", ImmediateDescription.of(s))
        .build()

      implicit val context = newOperatorCompilerContext("flowId")

      val thisType = OperatorCompiler.compile(operator, OperatorType.ExtractType)
      val cls = context.loadClass[Fragment[Input]](thisType.getClassName)

      val out1 = new GenericOutputFragment[Input]()
      val out2 = new GenericOutputFragment[Output]()

      val fragment = cls.getConstructor(
        classOf[Map[BroadcastId, Broadcasted[_]]],
        classOf[Fragment[_]], classOf[Fragment[_]]).newInstance(Map.empty, out1, out2)
      fragment.reset()

      val input = new Input()
      for (i <- 0 until 10) {
        input.i.modify(i)
        input.l.modify(i)
        fragment.add(input)
      }
      out1.iterator.zipWithIndex.foreach {
        case (input, i) =>
          assert(input.i.get === i)
          assert(input.l.get === i)
      }
      out2.iterator.zipWithIndex.foreach {
        case (output, i) =>
          assert(output.l.get === 10 * i)
          if (s == null) {
            assert(output.s.isNull)
          } else {
            assert(output.s.getAsString === s)
          }
      }
      fragment.reset()
    }
  }

  it should "compile Convert operator with projective model" in {
    val operator = OperatorExtractor
      .extract(classOf[Convert], classOf[ConvertOperator], "convertp")
      .input("input", ClassDescription.of(classOf[Input]))
      .output("original", ClassDescription.of(classOf[Input]))
      .output("out", ClassDescription.of(classOf[Output]))
      .argument("n", ImmediateDescription.of(10))
      .build()

    implicit val context = newOperatorCompilerContext("flowId")

    val thisType = OperatorCompiler.compile(operator, OperatorType.ExtractType)
    val cls = context.loadClass[Fragment[Input]](thisType.getClassName)

    val out1 = new GenericOutputFragment[Input]()
    val out2 = new GenericOutputFragment[Output]()

    val fragment = cls.getConstructor(
      classOf[Map[BroadcastId, Broadcasted[_]]],
      classOf[Fragment[_]], classOf[Fragment[_]]).newInstance(Map.empty, out1, out2)

    fragment.reset()
    val input = new Input()
    for (i <- 0 until 10) {
      input.i.modify(i)
      input.l.modify(i)
      fragment.add(input)
    }
    out1.iterator.zipWithIndex.foreach {
      case (input, i) =>
        assert(input.i.get === i)
        assert(input.l.get === i)
    }
    out2.iterator.zipWithIndex.foreach {
      case (output, i) =>
        assert(output.l.get === 10 * i)
        assert(output.s.isNull)
    }

    fragment.reset()
  }

  it should "compile Convert operator modifying original port" in {
    val operator = OperatorExtractor
      .extract(classOf[Convert], classOf[ConvertOperator], "convertp")
      .input("input", ClassDescription.of(classOf[Input]))
      .output("original", ClassDescription.of(classOf[Input]))
      .output("out", ClassDescription.of(classOf[Output]))
      .argument("n", ImmediateDescription.of(10))
      .build()

    implicit val context = newOperatorCompilerContext("flowId")

    val thisType = OperatorCompiler.compile(operator, OperatorType.ExtractType)
    val cls = context.loadClass[Fragment[Input]](thisType.getClassName)

    val out1 = new Fragment[Input] {

      val output = new GenericOutputFragment[Input]()

      override def doAdd(result: Input): Unit = {
        result.l.add(10)
        output.add(result)
      }

      override def doReset(): Unit = {
        output.reset()
      }
    }

    val out2 = new GenericOutputFragment[Output]()

    val fragment = cls.getConstructor(
      classOf[Map[BroadcastId, Broadcasted[_]]],
      classOf[Fragment[_]], classOf[Fragment[_]]).newInstance(Map.empty, out1, out2)

    fragment.reset()
    val input = new Input()
    for (i <- 0 until 10) {
      input.i.modify(i)
      input.l.modify(i)
      fragment.add(input)
    }
    out1.output.iterator.zipWithIndex.foreach {
      case (input, i) =>
        assert(input.i.get === i)
        assert(input.l.get === i + 10)
    }
    out2.iterator.zipWithIndex.foreach {
      case (output, i) =>
        assert(output.l.get === 10 * i)
        assert(output.s.isNull)
    }

    fragment.reset()
  }

  it should "compile Convert operator with view" in {
    val vMarker = MarkerOperator.builder(ClassDescription.of(classOf[Input]))
      .attribute(classOf[PlanMarker], PlanMarker.BROADCAST).build()
    val gvMarker = MarkerOperator.builder(ClassDescription.of(classOf[Input]))
      .attribute(classOf[PlanMarker], PlanMarker.BROADCAST).build()

    val operator = OperatorExtractor
      .extract(classOf[Convert], classOf[ConvertOperator], "convertWithView")
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
      .output("original", ClassDescription.of(classOf[Input]))
      .output("out", ClassDescription.of(classOf[Output]))
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

    val out1 = new GenericOutputFragment[Input]()
    val out2 = new GenericOutputFragment[Output]()

    val view = new MockBroadcast(0, Map(ShuffleKey.empty -> Seq(new Input())))
    val groupview = new MockBroadcast(1,
      (0 until 10).map { i =>
        val input = new Input()
        input.i.modify(i)
        new ShuffleKey(WritableSerDe.serialize(input.i)) -> Seq(input)
      }.toMap)

    val fragment = cls.getConstructor(
      classOf[Map[BroadcastId, Broadcasted[_]]],
      classOf[Fragment[_]], classOf[Fragment[_]])
      .newInstance(
        Map(
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
      case (input, i) =>
        assert(input.i.get === i)
        assert(input.l.get === i)
    }
    out2.iterator.zipWithIndex.foreach {
      case (output, i) =>
        assert(output.l.get === 10 * i)
        assert(output.s.isNull)
    }
    fragment.reset()
  }
}

object ConvertOperatorCompilerSpec {

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

  class Output extends DataModel[Output] with Writable {

    val l: LongOption = new LongOption()
    val s: StringOption = new StringOption()

    override def reset: Unit = {
      l.setNull()
      s.setNull()
    }
    override def copyFrom(other: Output): Unit = {
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

  class ConvertOperator {

    private[this] val out = new Output()

    @Convert
    def convert(in: Input, n: Int, s: String): Output = {
      out.reset()
      out.l.modify(n * in.l.get)
      if (s != null) {
        out.s.modify(s)
      }
      out
    }

    @Convert
    def convertp[I <: InputP](in: I, n: Int): Output = {
      out.reset()
      out.getLOption.modify(n * in.getLOption.get)
      out
    }

    @Convert
    def convertWithView(in: Input, v: View[Input], gv: GroupView[Input], n: Int): Output = {
      out.reset()
      out.l.modify(n * in.l.get)
      out
    }
  }
}
