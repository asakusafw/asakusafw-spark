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

import scala.collection.JavaConversions._
import scala.collection.mutable

import org.apache.spark.broadcast.Broadcast

import com.asakusafw.lang.compiler.api.CompilerOptions
import com.asakusafw.lang.compiler.api.testing.MockJobflowProcessorContext
import com.asakusafw.lang.compiler.model.description._
import com.asakusafw.lang.compiler.model.testing.OperatorExtractor
import com.asakusafw.runtime.model.DataModel
import com.asakusafw.runtime.value._
import com.asakusafw.spark.compiler.spi.{ OperatorCompiler, OperatorType }
import com.asakusafw.spark.compiler.subplan.{ BranchKeysClassBuilder, BroadcastIdsClassBuilder }
import com.asakusafw.spark.runtime.driver.BroadcastId
import com.asakusafw.spark.runtime.fragment._
import com.asakusafw.spark.tools.asm._
import com.asakusafw.vocabulary.operator.Update

@RunWith(classOf[JUnitRunner])
class UpdateOperatorCompilerSpecTest extends UpdateOperatorCompilerSpec

class UpdateOperatorCompilerSpec extends FlatSpec with LoadClassSugar with TempDir with CompilerContext {

  import UpdateOperatorCompilerSpec._

  behavior of classOf[UpdateOperatorCompiler].getSimpleName

  it should "compile Update operator" in {
    val operator = OperatorExtractor.extract(
      classOf[Update], classOf[UpdateOperator], "update")
      .input("in", ClassDescription.of(classOf[TestModel]))
      .output("out", ClassDescription.of(classOf[TestModel]))
      .argument("rate", ImmediateDescription.of(100))
      .build();

    val classpath = createTempDirectory("UpdateOperatorCompilerSpec").toFile
    implicit val context = newContext("flowId", classpath)

    val thisType = OperatorCompiler.compile(operator, OperatorType.ExtractType)
    val cls = loadClass(thisType.getClassName, classpath)
      .asSubclass(classOf[Fragment[TestModel]])

    val out = new GenericOutputFragment[TestModel]

    val fragment = cls
      .getConstructor(classOf[Map[BroadcastId, Broadcast[_]]], classOf[Fragment[_]])
      .newInstance(Map.empty, out)

    val dm = new TestModel()
    for (i <- 0 until 10) {
      dm.reset()
      dm.i.modify(i)
      fragment.add(dm)
    }
    assert(out.size === 10)
    out.zipWithIndex.foreach {
      case (dm, i) =>
        assert(dm.i.get === i)
        assert(dm.l.get === i * 100)
    }
    fragment.reset()
    assert(out.size === 0)
  }

  it should "compile Update operator with projective model" in {
    val operator = OperatorExtractor.extract(
      classOf[Update], classOf[UpdateOperator], "updatep")
      .input("in", ClassDescription.of(classOf[TestModel]))
      .output("out", ClassDescription.of(classOf[TestModel]))
      .argument("rate", ImmediateDescription.of(100))
      .build();

    val classpath = createTempDirectory("UpdateOperatorCompilerSpec").toFile
    implicit val context = newContext("flowId", classpath)

    val thisType = OperatorCompiler.compile(operator, OperatorType.ExtractType)
    val cls = loadClass(thisType.getClassName, classpath)
      .asSubclass(classOf[Fragment[TestModel]])

    val out = new GenericOutputFragment[TestModel]

    val fragment = cls
      .getConstructor(classOf[Map[BroadcastId, Broadcast[_]]], classOf[Fragment[_]])
      .newInstance(Map.empty, out)

    val dm = new TestModel()
    for (i <- 0 until 10) {
      dm.reset()
      dm.i.modify(i)
      fragment.add(dm)
    }
    assert(out.size === 10)
    out.zipWithIndex.foreach {
      case (dm, i) =>
        assert(dm.i.get === i)
        assert(dm.l.get === i * 100)
    }
    fragment.reset()
    assert(out.size === 0)
  }
}

object UpdateOperatorCompilerSpec {

  trait TestP {
    def getIOption: IntOption
    def getLOption: LongOption
  }

  class TestModel extends DataModel[TestModel] with TestP {

    val i: IntOption = new IntOption()
    val l: LongOption = new LongOption()

    override def reset: Unit = {
      i.setNull()
      l.setNull()
    }

    override def copyFrom(other: TestModel): Unit = {
      i.copyFrom(other.i)
      l.copyFrom(other.l)
    }

    def getIOption: IntOption = i
    def getLOption: LongOption = l
  }

  class UpdateOperator {

    @Update
    def update(item: TestModel, rate: Int): Unit = {
      item.l.modify(rate * item.i.get)
    }

    @Update
    def updatep[T <: TestP](item: T, rate: Int): Unit = {
      item.getLOption.modify(rate * item.getIOption.get)
    }
  }
}
