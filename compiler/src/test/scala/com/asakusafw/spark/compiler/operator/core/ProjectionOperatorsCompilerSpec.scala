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
package com.asakusafw.spark.compiler
package operator
package core

import org.junit.runner.RunWith
import org.scalatest.FlatSpec
import org.scalatest.junit.JUnitRunner

import java.io.{ DataInput, DataOutput }

import scala.collection.JavaConversions._
import scala.collection.mutable

import org.apache.hadoop.io.Writable
import org.apache.spark.broadcast.Broadcast

import com.asakusafw.lang.compiler.api.CompilerOptions
import com.asakusafw.lang.compiler.api.testing.MockJobflowProcessorContext
import com.asakusafw.lang.compiler.api.reference.DataModelReference
import com.asakusafw.lang.compiler.model.description.ClassDescription
import com.asakusafw.lang.compiler.model.graph.CoreOperator
import com.asakusafw.lang.compiler.model.graph.CoreOperator.CoreOperatorKind
import com.asakusafw.runtime.model.DataModel
import com.asakusafw.runtime.value._
import com.asakusafw.spark.compiler.spi.{ OperatorCompiler, OperatorType }
import com.asakusafw.spark.compiler.subplan.{ BranchKeysClassBuilder, BroadcastIdsClassBuilder }
import com.asakusafw.spark.runtime.driver.BroadcastId
import com.asakusafw.spark.runtime.fragment._
import com.asakusafw.spark.tools.asm._

@RunWith(classOf[JUnitRunner])
class ProjectionOperatorsCompilerSpecTest extends ProjectionOperatorsCompilerSpec

class ProjectionOperatorsCompilerSpec extends FlatSpec with UsingCompilerContext {

  import ProjectionOperatorsCompilerSpec._

  behavior of classOf[ProjectionOperatorsCompiler].getSimpleName

  it should "compile Project operator" in {
    val operator = CoreOperator.builder(CoreOperatorKind.PROJECT)
      .input("input", ClassDescription.of(classOf[ProjectInputModel]))
      .output("output", ClassDescription.of(classOf[ProjectOutputModel]))
      .build()

    implicit val context = newOperatorCompilerContext("flowId")

    val thisType = OperatorCompiler.compile(operator, OperatorType.ExtractType)
    val cls = context.loadClass[Fragment[ProjectInputModel]](thisType.getClassName)

    val out = new GenericOutputFragment[ProjectOutputModel]

    val fragment = cls
      .getConstructor(classOf[Map[BroadcastId, Broadcast[_]]], classOf[Fragment[_]])
      .newInstance(Map.empty, out)

    val dm = new ProjectInputModel()
    for (i <- 0 until 10) {
      dm.i.modify(i)
      dm.l.modify(i)
      fragment.add(dm)
    }
    out.iterator.zipWithIndex.foreach {
      case (dm, i) =>
        assert(dm.i.get === i)
    }
    fragment.reset()
  }

  it should "compile Extend operator" in {
    val operator = CoreOperator.builder(CoreOperatorKind.EXTEND)
      .input("input", ClassDescription.of(classOf[ExtendInputModel]))
      .output("output", ClassDescription.of(classOf[ExtendOutputModel]))
      .build()

    implicit val context = newOperatorCompilerContext("flowId")

    val thisType = OperatorCompiler.compile(operator, OperatorType.ExtractType)
    val cls = context.loadClass[Fragment[ExtendInputModel]](thisType.getClassName)

    val out = new GenericOutputFragment[ExtendOutputModel]

    val fragment = cls
      .getConstructor(classOf[Map[BroadcastId, Broadcast[_]]], classOf[Fragment[_]])
      .newInstance(Map.empty, out)

    val dm = new ExtendInputModel()
    for (i <- 0 until 10) {
      dm.i.modify(i)
      fragment.add(dm)
    }
    out.iterator.zipWithIndex.foreach {
      case (dm, i) =>
        assert(dm.i.get === i)
        assert(dm.l.isNull)
    }
    fragment.reset()
  }

  it should "compile Restructure operator" in {
    val operator = CoreOperator.builder(CoreOperatorKind.RESTRUCTURE)
      .input("input", ClassDescription.of(classOf[RestructureInputModel]))
      .output("output", ClassDescription.of(classOf[RestructureOutputModel]))
      .build()

    implicit val context = newOperatorCompilerContext("flowId")

    val thisType = OperatorCompiler.compile(operator, OperatorType.ExtractType)
    val cls = context.loadClass[Fragment[RestructureInputModel]](thisType.getClassName)

    val out = new GenericOutputFragment[RestructureOutputModel]

    val fragment = cls
      .getConstructor(classOf[Map[BroadcastId, Broadcast[_]]], classOf[Fragment[_]])
      .newInstance(Map.empty, out)

    fragment.reset()
    val dm = new RestructureInputModel()
    for (i <- 0 until 10) {
      dm.i.modify(i)
      fragment.add(dm)
    }
    out.iterator.zipWithIndex.foreach {
      case (dm, i) =>
        assert(dm.i.get === i)
        assert(dm.d.isNull)
    }

    fragment.reset()
  }
}

object ProjectionOperatorsCompilerSpec {

  class ProjectInputModel extends DataModel[ProjectInputModel] with Writable {

    val i: IntOption = new IntOption()
    val l: LongOption = new LongOption()

    override def reset: Unit = {
      i.setNull()
      l.setNull()
    }
    override def copyFrom(other: ProjectInputModel): Unit = {
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

  class ProjectOutputModel extends DataModel[ProjectOutputModel] with Writable {

    val i: IntOption = new IntOption()

    override def reset: Unit = {
      i.setNull()
    }
    override def copyFrom(other: ProjectOutputModel): Unit = {
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

  class ExtendInputModel extends DataModel[ExtendInputModel] with Writable {

    val i: IntOption = new IntOption()

    override def reset: Unit = {
      i.setNull()
    }
    override def copyFrom(other: ExtendInputModel): Unit = {
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

  class ExtendOutputModel extends DataModel[ExtendOutputModel] with Writable {

    val i: IntOption = new IntOption()
    val l: LongOption = new LongOption()

    override def reset: Unit = {
      i.setNull()
      l.setNull()
    }
    override def copyFrom(other: ExtendOutputModel): Unit = {
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

  class RestructureInputModel extends DataModel[RestructureInputModel] with Writable {

    val i: IntOption = new IntOption()
    val l: LongOption = new LongOption()

    override def reset: Unit = {
      i.setNull()
      l.setNull()
    }
    override def copyFrom(other: RestructureInputModel): Unit = {
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

  class RestructureOutputModel extends DataModel[RestructureOutputModel] with Writable {

    val i: IntOption = new IntOption()
    val d: DoubleOption = new DoubleOption()

    override def reset: Unit = {
      i.setNull()
      d.setNull()
    }
    override def copyFrom(other: RestructureOutputModel): Unit = {
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
