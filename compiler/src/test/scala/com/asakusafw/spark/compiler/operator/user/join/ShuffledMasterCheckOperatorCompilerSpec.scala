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
package user.join

import org.junit.runner.RunWith
import org.scalatest.FlatSpec
import org.scalatest.junit.JUnitRunner

import java.io.{ DataInput, DataOutput }
import java.util.{ List => JList }

import scala.collection.JavaConversions._
import scala.collection.mutable

import org.apache.hadoop.io.Writable
import org.apache.spark.broadcast.Broadcast

import com.asakusafw.lang.compiler.api.CompilerOptions
import com.asakusafw.lang.compiler.api.testing.MockJobflowProcessorContext
import com.asakusafw.lang.compiler.model.PropertyName
import com.asakusafw.lang.compiler.model.description._
import com.asakusafw.lang.compiler.model.graph.Groups
import com.asakusafw.lang.compiler.model.testing.OperatorExtractor
import com.asakusafw.runtime.core.Result
import com.asakusafw.runtime.model.DataModel
import com.asakusafw.runtime.value._
import com.asakusafw.spark.compiler.spi.{ OperatorCompiler, OperatorType }
import com.asakusafw.spark.compiler.subplan.{ BranchKeysClassBuilder, BroadcastIdsClassBuilder }
import com.asakusafw.spark.runtime.driver.BroadcastId
import com.asakusafw.spark.runtime.fragment._
import com.asakusafw.spark.tools.asm._
import com.asakusafw.vocabulary.operator.{ MasterCheck => MasterCheckOp, MasterSelection }

@RunWith(classOf[JUnitRunner])
class ShuffledMasterCheckOperatorCompilerSpecTest extends ShuffledMasterCheckOperatorCompilerSpec

class ShuffledMasterCheckOperatorCompilerSpec extends FlatSpec with LoadClassSugar with TempDir with UsingCompilerContext {

  import ShuffledMasterCheckOperatorCompilerSpec._

  behavior of classOf[ShuffledMasterCheckOperatorCompiler].getSimpleName

  it should "compile MasterCheck operator without master selection" in {
    val operator = OperatorExtractor
      .extract(classOf[MasterCheckOp], classOf[MasterCheckOperator], "check")
      .input("hoges", ClassDescription.of(classOf[Hoge]),
        Groups.parse(Seq("id")))
      .input("foos", ClassDescription.of(classOf[Foo]),
        Groups.parse(Seq("hogeId"), Seq("+id")))
      .output("found", ClassDescription.of(classOf[Foo]))
      .output("missed", ClassDescription.of(classOf[Foo]))
      .build()

    val classpath = createTempDirectory("MasterCheckOperatorCompilerSpec").toFile
    implicit val context = newContext("flowId", classpath)

    val thisType = OperatorCompiler.compile(operator, OperatorType.CoGroupType)(
      context.subplanCompilerContext.operatorCompilerContext)
    val cls = loadClass(thisType.getClassName, classpath).asSubclass(classOf[Fragment[Seq[Iterator[_]]]])

    val found = new GenericOutputFragment[Foo]
    val missed = new GenericOutputFragment[Foo]

    val fragment = cls.getConstructor(
      classOf[Map[BroadcastId, Broadcast[_]]],
      classOf[Fragment[_]], classOf[Fragment[_]])
      .newInstance(Map.empty, found, missed)

    {
      fragment.reset()
      val hoge = new Hoge()
      hoge.id.modify(1)
      val hoges = Seq(hoge)
      val foo = new Foo()
      foo.id.modify(10)
      foo.hogeId.modify(1)
      val foos = Seq(foo)
      fragment.add(Seq(hoges.iterator, foos.iterator))
      val founds = found.iterator.toSeq
      assert(founds.size === 1)
      assert(founds.head.id.get === 10)
      val misseds = missed.iterator.toSeq
      assert(misseds.size === 0)
    }

    fragment.reset()
    assert(found.iterator.size === 0)
    assert(missed.iterator.size === 0)

    {
      fragment.reset()
      val hoges = Seq.empty[Hoge]
      val foo = new Foo()
      foo.id.modify(10)
      foo.hogeId.modify(1)
      val foos = Seq(foo)
      fragment.add(Seq(hoges.iterator, foos.iterator))
      val founds = found.iterator.toSeq
      assert(founds.size === 0)
      val misseds = missed.iterator.toSeq
      assert(misseds.size === 1)
      assert(misseds.head.id.get === 10)
    }

    fragment.reset()
    assert(found.iterator.size === 0)
    assert(missed.iterator.size === 0)
  }

  it should "compile MasterCheck operator with master selection" in {
    val operator = OperatorExtractor
      .extract(classOf[MasterCheckOp], classOf[MasterCheckOperator], "checkWithSelection")
      .input("hoges", ClassDescription.of(classOf[Hoge]),
        Groups.parse(Seq("id")))
      .input("foos", ClassDescription.of(classOf[Foo]),
        Groups.parse(Seq("hogeId"), Seq("+id")))
      .output("found", ClassDescription.of(classOf[Foo]))
      .output("missed", ClassDescription.of(classOf[Foo]))
      .build()

    val classpath = createTempDirectory("MasterCheckOperatorCompilerSpec").toFile
    implicit val context = newContext("flowId", classpath)

    val thisType = OperatorCompiler.compile(operator, OperatorType.CoGroupType)(
      context.subplanCompilerContext.operatorCompilerContext)
    val cls = loadClass(thisType.getClassName, classpath).asSubclass(classOf[Fragment[Seq[Iterator[_]]]])

    val found = new GenericOutputFragment[Foo]
    val missed = new GenericOutputFragment[Foo]

    val fragment = cls.getConstructor(
      classOf[Map[BroadcastId, Broadcast[_]]],
      classOf[Fragment[_]], classOf[Fragment[_]])
      .newInstance(Map.empty, found, missed)

    {
      fragment.reset()
      val hoge = new Hoge()
      hoge.id.modify(0)
      val hoges = Seq(hoge)
      val foos = (0 until 10).map { i =>
        val foo = new Foo()
        foo.id.modify(i)
        foo.hogeId.modify(0)
        foo
      }
      fragment.add(Seq(hoges.iterator, foos.iterator))
      val founds = found.iterator.toSeq
      assert(founds.size === 5)
      assert(founds.map(_.id.get) === (0 until 10 by 2))
      val misseds = missed.iterator.toSeq
      assert(misseds.size === 5)
      assert(misseds.map(_.id.get) === (1 until 10 by 2))
    }

    fragment.reset()
    assert(found.iterator.size === 0)
    assert(missed.iterator.size === 0)

    {
      fragment.reset()
      val hoges = Seq.empty[Hoge]
      val foo = new Foo()
      foo.id.modify(10)
      foo.hogeId.modify(1)
      val foos = Seq(foo)
      fragment.add(Seq(hoges.iterator, foos.iterator))
      val founds = found.iterator.toSeq
      assert(founds.size === 0)
      val misseds = missed.iterator.toSeq
      assert(misseds.size === 1)
      assert(misseds.head.id.get === 10)
    }

    fragment.reset()
    assert(found.iterator.size === 0)
    assert(missed.iterator.size === 0)
  }
}

object ShuffledMasterCheckOperatorCompilerSpec {

  class Hoge extends DataModel[Hoge] with Writable {

    val id = new IntOption()

    override def reset(): Unit = {
      id.setNull()
    }
    override def copyFrom(other: Hoge): Unit = {
      id.copyFrom(other.id)
    }
    override def readFields(in: DataInput): Unit = {
      id.readFields(in)
    }
    override def write(out: DataOutput): Unit = {
      id.write(out)
    }

    def getIdOption: IntOption = id
  }

  class Foo extends DataModel[Foo] with Writable {

    val id = new IntOption()
    val hogeId = new IntOption()

    override def reset(): Unit = {
      id.setNull()
      hogeId.setNull()
    }
    override def copyFrom(other: Foo): Unit = {
      id.copyFrom(other.id)
      hogeId.copyFrom(other.hogeId)
    }
    override def readFields(in: DataInput): Unit = {
      id.readFields(in)
      hogeId.readFields(in)
    }
    override def write(out: DataOutput): Unit = {
      id.write(out)
      hogeId.write(out)
    }

    def getIdOption: IntOption = id
    def getHogeIdOption: IntOption = hogeId
  }

  class MasterCheckOperator {

    @MasterCheckOp
    def check(hoge: Hoge, foo: Foo): Boolean = ???

    @MasterCheckOp(selection = "select")
    def checkWithSelection(hoge: Hoge, foo: Foo): Boolean = ???

    @MasterSelection
    def select(hoges: JList[Hoge], foo: Foo): Hoge = {
      if (foo.id.get % 2 == 0) {
        hoges.headOption.orNull
      } else {
        null
      }
    }
  }
}
