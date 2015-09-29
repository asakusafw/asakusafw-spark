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
package join

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
import com.asakusafw.vocabulary.operator.{ MasterBranch => MasterBranchOp, MasterSelection }

@RunWith(classOf[JUnitRunner])
class ShuffledMasterBranchOperatorCompilerSpecTest extends ShuffledMasterBranchOperatorCompilerSpec

class ShuffledMasterBranchOperatorCompilerSpec extends FlatSpec with LoadClassSugar with TempDir with UsingCompilerContext {

  import ShuffledMasterBranchOperatorCompilerSpec._

  behavior of classOf[ShuffledMasterBranchOperatorCompiler].getSimpleName

  it should "compile MasterBranch operator without master selection" in {
    val operator = OperatorExtractor
      .extract(classOf[MasterBranchOp], classOf[MasterBranchOperator], "branch")
      .input("hoges", ClassDescription.of(classOf[Hoge]),
        Groups.parse(Seq("id")))
      .input("foos", ClassDescription.of(classOf[Foo]),
        Groups.parse(Seq("hogeId"), Seq("+id")))
      .output("low", ClassDescription.of(classOf[Foo]))
      .output("high", ClassDescription.of(classOf[Foo]))
      .build()

    val classpath = createTempDirectory("MasterBranchOperatorCompilerSpec").toFile
    implicit val context = newContext("flowId", classpath)

    val thisType = OperatorCompiler.compile(operator, OperatorType.CoGroupType)
    val cls = loadClass(thisType.getClassName, classpath).asSubclass(classOf[Fragment[Seq[Iterator[_]]]])

    val low = new GenericOutputFragment[Foo]
    val high = new GenericOutputFragment[Foo]

    val fragment = cls.getConstructor(
      classOf[Map[BroadcastId, Broadcast[_]]],
      classOf[Fragment[_]], classOf[Fragment[_]])
      .newInstance(Map.empty, low, high)

    {
      fragment.reset()
      val hoge = new Hoge()
      hoge.id.modify(10)
      val hoges = Seq(hoge)
      val foo = new Foo()
      foo.id.modify(10)
      foo.hogeId.modify(10)
      val foos = Seq(foo)
      fragment.add(Seq(hoges.iterator, foos.iterator))
      val lows = low.iterator.toSeq
      assert(lows.size === 0)
      val highs = high.iterator.toSeq
      assert(highs.size === 1)
      assert(highs.head.id.get === 10)
    }

    fragment.reset()
    assert(low.iterator.size === 0)
    assert(high.iterator.size === 0)

    {
      fragment.reset()
      val hoges = Seq.empty[Hoge]
      val foo = new Foo()
      foo.id.modify(10)
      foo.hogeId.modify(1)
      val foos = Seq(foo)
      fragment.add(Seq(hoges.iterator, foos.iterator))
      val lows = low.iterator.toSeq
      assert(lows.size === 1)
      assert(lows.head.id.get === 10)
      val highs = high.iterator.toSeq
      assert(highs.size === 0)
    }

    fragment.reset()
    assert(low.iterator.size === 0)
    assert(high.iterator.size === 0)
  }

  it should "compile MasterBranch operator with master selection" in {
    val operator = OperatorExtractor
      .extract(classOf[MasterBranchOp], classOf[MasterBranchOperator], "branchWithSelection")
      .input("hoges", ClassDescription.of(classOf[Hoge]),
        Groups.parse(Seq("id")))
      .input("foos", ClassDescription.of(classOf[Foo]),
        Groups.parse(Seq("hogeId"), Seq("+id")))
      .output("low", ClassDescription.of(classOf[Foo]))
      .output("high", ClassDescription.of(classOf[Foo]))
      .build()

    val classpath = createTempDirectory("MasterBranchOperatorCompilerSpec").toFile
    implicit val context = newContext("flowId", classpath)

    val thisType = OperatorCompiler.compile(operator, OperatorType.CoGroupType)
    val cls = loadClass(thisType.getClassName, classpath).asSubclass(classOf[Fragment[Seq[Iterator[_]]]])

    val low = new GenericOutputFragment[Foo]
    val high = new GenericOutputFragment[Foo]

    val fragment = cls.getConstructor(
      classOf[Map[BroadcastId, Broadcast[_]]],
      classOf[Fragment[_]], classOf[Fragment[_]])
      .newInstance(Map.empty, low, high)

    {
      fragment.reset()
      val hoge = new Hoge()
      hoge.id.modify(10)
      val hoges = Seq(hoge)
      val foos = (0 until 10).map { i =>
        val foo = new Foo()
        foo.id.modify(i)
        foo.hogeId.modify(10)
        foo
      }
      fragment.add(Seq(hoges.iterator, foos.iterator))
      val lows = low.iterator.toSeq
      assert(lows.size === 5)
      assert(lows.map(_.id.get) === (1 until 10 by 2))
      val highs = high.iterator.toSeq
      assert(highs.size === 5)
      assert(highs.map(_.id.get) === (0 until 10 by 2))
    }

    fragment.reset()
    assert(low.iterator.size === 0)
    assert(high.iterator.size === 0)

    {
      fragment.reset()
      val hoges = Seq.empty[Hoge]
      val foo = new Foo()
      foo.id.modify(10)
      foo.hogeId.modify(1)
      val foos = Seq(foo)
      fragment.add(Seq(hoges.iterator, foos.iterator))
      val lows = low.iterator.toSeq
      assert(lows.size === 1)
      assert(lows.head.id.get === 10)
      val highs = high.iterator.toSeq
      assert(highs.size === 0)
    }

    fragment.reset()
    assert(low.iterator.size === 0)
    assert(high.iterator.size === 0)
  }

  it should "compile MasterBranch operator without master selection with projective model" in {
    val operator = OperatorExtractor
      .extract(classOf[MasterBranchOp], classOf[MasterBranchOperator], "branchp")
      .input("hoges", ClassDescription.of(classOf[Hoge]),
        Groups.parse(Seq("id")))
      .input("foos", ClassDescription.of(classOf[Foo]),
        Groups.parse(Seq("hogeId"), Seq("+id")))
      .output("low", ClassDescription.of(classOf[Foo]))
      .output("high", ClassDescription.of(classOf[Foo]))
      .build()

    val classpath = createTempDirectory("MasterBranchOperatorCompilerSpec").toFile
    implicit val context = newContext("flowId", classpath)

    val thisType = OperatorCompiler.compile(operator, OperatorType.CoGroupType)
    val cls = loadClass(thisType.getClassName, classpath).asSubclass(classOf[Fragment[Seq[Iterator[_]]]])

    val low = new GenericOutputFragment[Foo]
    val high = new GenericOutputFragment[Foo]

    val fragment = cls.getConstructor(
      classOf[Map[BroadcastId, Broadcast[_]]],
      classOf[Fragment[_]], classOf[Fragment[_]])
      .newInstance(Map.empty, low, high)

    {
      fragment.reset()
      val hoge = new Hoge()
      hoge.id.modify(10)
      val hoges = Seq(hoge)
      val foo = new Foo()
      foo.id.modify(10)
      foo.hogeId.modify(10)
      val foos = Seq(foo)
      fragment.add(Seq(hoges.iterator, foos.iterator))
      val lows = low.iterator.toSeq
      assert(lows.size === 0)
      val highs = high.iterator.toSeq
      assert(highs.size === 1)
      assert(highs.head.id.get === 10)
    }

    fragment.reset()
    assert(low.iterator.size === 0)
    assert(high.iterator.size === 0)

    {
      fragment.reset()
      val hoges = Seq.empty[Hoge]
      val foo = new Foo()
      foo.id.modify(10)
      foo.hogeId.modify(1)
      val foos = Seq(foo)
      fragment.add(Seq(hoges.iterator, foos.iterator))
      val lows = low.iterator.toSeq
      assert(lows.size === 1)
      assert(lows.head.id.get === 10)
      val highs = high.iterator.toSeq
      assert(highs.size === 0)
    }

    fragment.reset()
    assert(low.iterator.size === 0)
    assert(high.iterator.size === 0)
  }

  it should "compile MasterBranch operator with master selection with projective model" in {
    val operator = OperatorExtractor
      .extract(classOf[MasterBranchOp], classOf[MasterBranchOperator], "branchWithSelectionp")
      .input("hoges", ClassDescription.of(classOf[Hoge]),
        Groups.parse(Seq("id")))
      .input("foos", ClassDescription.of(classOf[Foo]),
        Groups.parse(Seq("hogeId"), Seq("+id")))
      .output("low", ClassDescription.of(classOf[Foo]))
      .output("high", ClassDescription.of(classOf[Foo]))
      .build()

    val classpath = createTempDirectory("MasterBranchOperatorCompilerSpec").toFile
    implicit val context = newContext("flowId", classpath)

    val thisType = OperatorCompiler.compile(operator, OperatorType.CoGroupType)
    val cls = loadClass(thisType.getClassName, classpath).asSubclass(classOf[Fragment[Seq[Iterator[_]]]])

    val low = new GenericOutputFragment[Foo]
    val high = new GenericOutputFragment[Foo]

    val fragment = cls.getConstructor(
      classOf[Map[BroadcastId, Broadcast[_]]],
      classOf[Fragment[_]], classOf[Fragment[_]])
      .newInstance(Map.empty, low, high)

    {
      fragment.reset()
      val hoge = new Hoge()
      hoge.id.modify(10)
      val hoges = Seq(hoge)
      val foos = (0 until 10).map { i =>
        val foo = new Foo()
        foo.id.modify(i)
        foo.hogeId.modify(10)
        foo
      }
      fragment.add(Seq(hoges.iterator, foos.iterator))
      val lows = low.iterator.toSeq
      assert(lows.size === 5)
      assert(lows.map(_.id.get) === (1 until 10 by 2))
      val highs = high.iterator.toSeq
      assert(highs.size === 5)
      assert(highs.map(_.id.get) === (0 until 10 by 2))
    }

    fragment.reset()
    assert(low.iterator.size === 0)
    assert(high.iterator.size === 0)

    {
      fragment.reset()
      val hoges = Seq.empty[Hoge]
      val foo = new Foo()
      foo.id.modify(10)
      foo.hogeId.modify(1)
      val foos = Seq(foo)
      fragment.add(Seq(hoges.iterator, foos.iterator))
      val lows = low.iterator.toSeq
      assert(lows.size === 1)
      assert(lows.head.id.get === 10)
      val highs = high.iterator.toSeq
      assert(highs.size === 0)
    }

    fragment.reset()
    assert(low.iterator.size === 0)
    assert(high.iterator.size === 0)
  }
}

object ShuffledMasterBranchOperatorCompilerSpec {

  trait HogeP {
    def getIdOption: IntOption
  }

  class Hoge extends DataModel[Hoge] with HogeP with Writable {

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

  trait FooP {
    def getIdOption: IntOption
    def getHogeIdOption: IntOption
  }

  class Foo extends DataModel[Foo] with FooP with Writable {

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

  class MasterBranchOperator {

    @MasterBranchOp
    def branch(hoge: Hoge, foo: Foo): BranchOperatorCompilerSpecTestBranch = {
      if (hoge == null || hoge.id.get < 5) {
        BranchOperatorCompilerSpecTestBranch.LOW
      } else {
        BranchOperatorCompilerSpecTestBranch.HIGH
      }
    }

    @MasterBranchOp(selection = "select")
    def branchWithSelection(hoge: Hoge, foo: Foo): BranchOperatorCompilerSpecTestBranch = {
      if (hoge == null || hoge.id.get < 5) {
        BranchOperatorCompilerSpecTestBranch.LOW
      } else {
        BranchOperatorCompilerSpecTestBranch.HIGH
      }
    }

    @MasterSelection
    def select(hoges: JList[Hoge], foo: Foo): Hoge = {
      if (foo.id.get % 2 == 0) {
        hoges.headOption.orNull
      } else {
        null
      }
    }

    @MasterBranchOp
    def branchp[H <: HogeP, F <: FooP](hoge: H, foo: F): BranchOperatorCompilerSpecTestBranch = {
      if (hoge == null || hoge.getIdOption.get < 5) {
        BranchOperatorCompilerSpecTestBranch.LOW
      } else {
        BranchOperatorCompilerSpecTestBranch.HIGH
      }
    }

    @MasterBranchOp(selection = "selectp")
    def branchWithSelectionp[H <: HogeP, F <: FooP](hoge: H, foo: F): BranchOperatorCompilerSpecTestBranch = {
      if (hoge == null || hoge.getIdOption.get < 5) {
        BranchOperatorCompilerSpecTestBranch.LOW
      } else {
        BranchOperatorCompilerSpecTestBranch.HIGH
      }
    }

    @MasterSelection
    def selectp[H <: HogeP, F <: FooP](hoges: JList[H], foo: F): H = {
      if (foo.getIdOption.get % 2 == 0) {
        if (hoges.size > 0) {
          hoges.head
        } else {
          null.asInstanceOf[H]
        }
      } else {
        null.asInstanceOf[H]
      }
    }
  }
}
