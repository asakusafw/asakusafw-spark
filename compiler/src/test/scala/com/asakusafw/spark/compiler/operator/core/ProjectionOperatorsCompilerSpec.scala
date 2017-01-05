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
package core

import org.junit.runner.RunWith
import org.scalatest.FlatSpec
import org.scalatest.junit.JUnitRunner

import java.io.{ DataInput, DataOutput }

import scala.collection.JavaConversions._

import org.apache.hadoop.io.Writable
import org.apache.spark.broadcast.Broadcast

import com.asakusafw.lang.compiler.model.description.ClassDescription
import com.asakusafw.lang.compiler.model.graph.CoreOperator
import com.asakusafw.lang.compiler.model.graph.CoreOperator.CoreOperatorKind
import com.asakusafw.runtime.model.DataModel
import com.asakusafw.runtime.value.{ DoubleOption, IntOption, LongOption }
import com.asakusafw.spark.compiler.spi.{ OperatorCompiler, OperatorType }
import com.asakusafw.spark.runtime.fragment.{ Fragment, GenericOutputFragment }
import com.asakusafw.spark.runtime.graph.BroadcastId
import com.asakusafw.spark.tools.asm._

@RunWith(classOf[JUnitRunner])
class ProjectionOperatorsCompilerSpecTest extends ProjectionOperatorsCompilerSpec

class ProjectionOperatorsCompilerSpec extends FlatSpec with UsingCompilerContext {

  import ProjectionOperatorsCompilerSpec._

  behavior of classOf[ProjectionOperatorsCompiler].getSimpleName

  it should "compile Project operator" in {
    import Project._

    val operator = CoreOperator.builder(CoreOperatorKind.PROJECT)
      .input("input", ClassDescription.of(classOf[Input]))
      .output("output", ClassDescription.of(classOf[Output]))
      .build()

    implicit val context = newOperatorCompilerContext("flowId")

    val thisType = OperatorCompiler.compile(operator, OperatorType.ExtractType)
    val cls = context.loadClass[Fragment[Input]](thisType.getClassName)

    val out = new GenericOutputFragment[Output]()

    val fragment = cls
      .getConstructor(classOf[Map[BroadcastId, Broadcast[_]]], classOf[Fragment[_]])
      .newInstance(Map.empty, out)

    val input = new Project.Input()
    for (i <- 0 until 10) {
      input.i.modify(i)
      input.l.modify(i)
      fragment.add(input)
    }
    out.iterator.zipWithIndex.foreach {
      case (output, i) =>
        assert(output.i.get === i)
    }
    fragment.reset()
  }

  it should "compile Extend operator" in {
    import Extend._

    val operator = CoreOperator.builder(CoreOperatorKind.EXTEND)
      .input("input", ClassDescription.of(classOf[Input]))
      .output("output", ClassDescription.of(classOf[Output]))
      .build()

    implicit val context = newOperatorCompilerContext("flowId")

    val thisType = OperatorCompiler.compile(operator, OperatorType.ExtractType)
    val cls = context.loadClass[Fragment[Input]](thisType.getClassName)

    val out = new GenericOutputFragment[Output]()

    val fragment = cls
      .getConstructor(classOf[Map[BroadcastId, Broadcast[_]]], classOf[Fragment[_]])
      .newInstance(Map.empty, out)

    val input = new Input()
    for (i <- 0 until 10) {
      input.i.modify(i)
      fragment.add(input)
    }
    out.iterator.zipWithIndex.foreach {
      case (output, i) =>
        assert(output.i.get === i)
        assert(output.l.isNull)
    }
    fragment.reset()
  }

  it should "compile Restructure operator" in {
    import Restructure._

    val operator = CoreOperator.builder(CoreOperatorKind.RESTRUCTURE)
      .input("input", ClassDescription.of(classOf[Input]))
      .output("output", ClassDescription.of(classOf[Output]))
      .build()

    implicit val context = newOperatorCompilerContext("flowId")

    val thisType = OperatorCompiler.compile(operator, OperatorType.ExtractType)
    val cls = context.loadClass[Fragment[Input]](thisType.getClassName)

    val out = new GenericOutputFragment[Output]()

    val fragment = cls
      .getConstructor(classOf[Map[BroadcastId, Broadcast[_]]], classOf[Fragment[_]])
      .newInstance(Map.empty, out)

    fragment.reset()
    val input = new Input()
    for (i <- 0 until 10) {
      input.i.modify(i)
      fragment.add(input)
    }
    out.iterator.zipWithIndex.foreach {
      case (output, i) =>
        assert(output.i.get === i)
        assert(output.d.isNull)
    }

    fragment.reset()
  }
}

object ProjectionOperatorsCompilerSpec {

  object Project {

    class Input extends DataModel[Input] with Writable {

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

      val i: IntOption = new IntOption()

      override def reset: Unit = {
        i.setNull()
      }
      override def copyFrom(other: Output): Unit = {
        i.copyFrom(other.i)
      }
      override def readFields(in: DataInput): Unit = {
        i.readFields(in)
      }
      override def write(out: DataOutput): Unit = {
        i.write(out)
      }

      def getIOption: IntOption = i
    }
  }

  object Extend {

    class Input extends DataModel[Input] with Writable {

      val i: IntOption = new IntOption()

      override def reset: Unit = {
        i.setNull()
      }
      override def copyFrom(other: Input): Unit = {
        i.copyFrom(other.i)
      }
      override def readFields(in: DataInput): Unit = {
        i.readFields(in)
      }
      override def write(out: DataOutput): Unit = {
        i.write(out)
      }

      def getIOption: IntOption = i
    }

    class Output extends DataModel[Output] with Writable {

      val i: IntOption = new IntOption()
      val l: LongOption = new LongOption()

      override def reset: Unit = {
        i.setNull()
        l.setNull()
      }
      override def copyFrom(other: Output): Unit = {
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
  }

  object Restructure {

    class Input extends DataModel[Input] with Writable {

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

      val i: IntOption = new IntOption()
      val d: DoubleOption = new DoubleOption()

      override def reset: Unit = {
        i.setNull()
        d.setNull()
      }
      override def copyFrom(other: Output): Unit = {
        i.copyFrom(other.i)
        d.copyFrom(other.d)
      }
      override def readFields(in: DataInput): Unit = {
        i.readFields(in)
        d.readFields(in)
      }
      override def write(out: DataOutput): Unit = {
        i.write(out)
        d.write(out)
      }

      def getIOption: IntOption = i
      def getDOption: DoubleOption = d
    }
  }
}
