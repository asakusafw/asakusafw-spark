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
package user

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
import com.asakusafw.lang.compiler.model.description._
import com.asakusafw.lang.compiler.model.testing.OperatorExtractor
import com.asakusafw.runtime.core.Result
import com.asakusafw.runtime.model.DataModel
import com.asakusafw.runtime.value._
import com.asakusafw.spark.compiler.spi.{ OperatorCompiler, OperatorType }
import com.asakusafw.spark.compiler.subplan.{ BranchKeysClassBuilder, BroadcastIdsClassBuilder }
import com.asakusafw.spark.runtime.driver.BroadcastId
import com.asakusafw.spark.runtime.fragment._
import com.asakusafw.spark.runtime.operator.GenericOutputFragment
import com.asakusafw.spark.tools.asm._
import com.asakusafw.vocabulary.operator.Branch

@RunWith(classOf[JUnitRunner])
class BranchOperatorCompilerSpecTest extends BranchOperatorCompilerSpec

class BranchOperatorCompilerSpec extends FlatSpec with UsingCompilerContext {

  import BranchOperatorCompilerSpec._

  behavior of classOf[BranchOperatorCompiler].getSimpleName

  it should "compile Branch operator" in {
    val operator = OperatorExtractor
      .extract(classOf[Branch], classOf[BranchOperator], "branch")
      .input("input", ClassDescription.of(classOf[InputModel]))
      .output("low", ClassDescription.of(classOf[InputModel]))
      .output("high", ClassDescription.of(classOf[InputModel]))
      .argument("n", ImmediateDescription.of(10))
      .build()

    implicit val context = newOperatorCompilerContext("flowId")

    val thisType = OperatorCompiler.compile(operator, OperatorType.ExtractType)
    val cls = context.loadClass[Fragment[InputModel]](thisType.getClassName)

    val out1 = new GenericOutputFragment[InputModel]
    val out2 = new GenericOutputFragment[InputModel]

    val fragment = cls
      .getConstructor(
        classOf[Map[BroadcastId, Broadcast[_]]],
        classOf[Fragment[_]], classOf[Fragment[_]])
      .newInstance(Map.empty, out1, out2)

    val dm = new InputModel()
    for (i <- 0 until 10) {
      dm.i.modify(i)
      fragment.add(dm)
    }
    out1.iterator.zipWithIndex.foreach {
      case (dm, i) =>
        assert(dm.i.get === i)
    }
    out2.iterator.zipWithIndex.foreach {
      case (dm, i) =>
        assert(dm.i.get === i + 5)
    }
    fragment.reset()
  }

  it should "compile Branch operator with projective model" in {
    val operator = OperatorExtractor
      .extract(classOf[Branch], classOf[BranchOperator], "branchp")
      .input("input", ClassDescription.of(classOf[InputModel]))
      .output("low", ClassDescription.of(classOf[InputModel]))
      .output("high", ClassDescription.of(classOf[InputModel]))
      .argument("n", ImmediateDescription.of(10))
      .build()

    implicit val context = newOperatorCompilerContext("flowId")

    val thisType = OperatorCompiler.compile(operator, OperatorType.ExtractType)
    val cls = context.loadClass[Fragment[InputModel]](thisType.getClassName)

    val out1 = new GenericOutputFragment[InputModel]
    val out2 = new GenericOutputFragment[InputModel]

    val fragment = cls
      .getConstructor(
        classOf[Map[BroadcastId, Broadcast[_]]],
        classOf[Fragment[_]], classOf[Fragment[_]])
      .newInstance(Map.empty, out1, out2)

    fragment.reset()
    val dm = new InputModel()
    for (i <- 0 until 10) {
      dm.i.modify(i)
      fragment.add(dm)
    }
    out1.iterator.zipWithIndex.foreach {
      case (dm, i) =>
        assert(dm.i.get === i)
    }
    out2.iterator.zipWithIndex.foreach {
      case (dm, i) =>
        assert(dm.i.get === i + 5)
    }

    fragment.reset()
  }
}

object BranchOperatorCompilerSpec {

  trait InputP {
    def getIOption: IntOption
  }

  class InputModel extends DataModel[InputModel] with InputP with Writable {

    val i: IntOption = new IntOption()

    override def reset: Unit = {
      i.setNull()
    }
    override def copyFrom(other: InputModel): Unit = {
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

  class BranchOperator {

    @Branch
    def branch(in: InputModel, n: Int): BranchOperatorCompilerSpecTestBranch = {
      if (in.i.get < 5) {
        BranchOperatorCompilerSpecTestBranch.LOW
      } else {
        BranchOperatorCompilerSpecTestBranch.HIGH
      }
    }

    @Branch
    def branchp[I <: InputP](in: I, n: Int): BranchOperatorCompilerSpecTestBranch = {
      if (in.getIOption.get < 5) {
        BranchOperatorCompilerSpecTestBranch.LOW
      } else {
        BranchOperatorCompilerSpecTestBranch.HIGH
      }
    }
  }
}
