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
import com.asakusafw.spark.runtime.operator.GenericOutputFragment
import com.asakusafw.spark.tools.asm._
import com.asakusafw.vocabulary.operator.{ MasterJoinUpdate => MasterJoinUpdateOp, MasterSelection }

@RunWith(classOf[JUnitRunner])
class ShuffledMasterJoinUpdateOperatorCompilerSpecTest extends ShuffledMasterJoinUpdateOperatorCompilerSpec

class ShuffledMasterJoinUpdateOperatorCompilerSpec extends FlatSpec with UsingCompilerContext {

  import ShuffledMasterJoinUpdateOperatorCompilerSpec._

  behavior of classOf[ShuffledMasterJoinUpdateOperatorCompiler].getSimpleName

  it should "compile MasterJoinUpdate operator without master selection" in {
    val operator = OperatorExtractor
      .extract(classOf[MasterJoinUpdateOp], classOf[MasterJoinUpdateOperator], "update")
      .input("hoges", ClassDescription.of(classOf[Hoge]),
        Groups.parse(Seq("id")))
      .input("foos", ClassDescription.of(classOf[Foo]),
        Groups.parse(Seq("hogeId"), Seq("+id")))
      .output("updated", ClassDescription.of(classOf[Foo]))
      .output("missed", ClassDescription.of(classOf[Foo]))
      .build()

    implicit val context = newOperatorCompilerContext("flowId")

    val thisType = OperatorCompiler.compile(operator, OperatorType.CoGroupType)
    val cls = context.loadClass[Fragment[Seq[Iterator[_]]]](thisType.getClassName)

    val updated = new GenericOutputFragment[Foo]
    val missed = new GenericOutputFragment[Foo]

    val fragment = cls
      .getConstructor(
        classOf[Map[BroadcastId, Broadcast[_]]],
        classOf[Fragment[_]], classOf[Fragment[_]])
      .newInstance(Map.empty, updated, missed)

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
      val updateds = updated.iterator.toSeq
      assert(updateds.size === 1)
      assert(updateds.head.id.get === 10)
      val misseds = missed.iterator.toSeq
      assert(misseds.size === 0)
    }

    fragment.reset()
    assert(updated.iterator.size === 0)
    assert(missed.iterator.size === 0)

    {
      fragment.reset()
      val hoges = Iterator.empty
      val foo = new Foo()
      foo.id.modify(10)
      foo.hogeId.modify(1)
      val foos = Iterator(foo)
      fragment.add(Seq(hoges, foos))
      val updateds = updated.iterator.toSeq
      assert(updateds.size === 0)
      val misseds = missed.iterator.toSeq
      assert(misseds.size === 1)
      assert(misseds.head.id.get === 10)
    }

    fragment.reset()
    assert(updated.iterator.size === 0)
    assert(missed.iterator.size === 0)
  }

  it should "compile MasterJoinUpdate operator with master selection" in {
    val operator = OperatorExtractor
      .extract(classOf[MasterJoinUpdateOp], classOf[MasterJoinUpdateOperator], "updateWithSelection")
      .input("hoges", ClassDescription.of(classOf[Hoge]),
        Groups.parse(Seq("id")))
      .input("foos", ClassDescription.of(classOf[Foo]),
        Groups.parse(Seq("hogeId"), Seq("+id")))
      .output("updated", ClassDescription.of(classOf[Foo]))
      .output("missed", ClassDescription.of(classOf[Foo]))
      .build()

    implicit val context = newOperatorCompilerContext("flowId")

    val thisType = OperatorCompiler.compile(operator, OperatorType.CoGroupType)
    val cls = context.loadClass[Fragment[Seq[Iterator[_]]]](thisType.getClassName)

    val updated = new GenericOutputFragment[Foo]
    val missed = new GenericOutputFragment[Foo]

    val fragment = cls
      .getConstructor(
        classOf[Map[BroadcastId, Broadcast[_]]],
        classOf[Fragment[_]], classOf[Fragment[_]])
      .newInstance(Map.empty, updated, missed)

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
      val updateds = updated.iterator.toSeq
      assert(updateds.size === 5)
      assert(updateds.map(_.id.get) === (0 until 10 by 2))
      val misseds = missed.iterator.toSeq
      assert(misseds.size === 5)
      assert(misseds.map(_.id.get) === (1 until 10 by 2))
    }

    fragment.reset()
    assert(updated.iterator.size === 0)
    assert(missed.iterator.size === 0)

    {
      fragment.reset()
      val hoges = Seq.empty[Hoge]
      val foo = new Foo()
      foo.id.modify(10)
      foo.hogeId.modify(1)
      val foos = Seq(foo)
      fragment.add(Seq(hoges.iterator, foos.iterator))
      val updateds = updated.iterator.toSeq
      assert(updateds.size === 0)
      val misseds = missed.iterator.toSeq
      assert(misseds.size === 1)
      assert(misseds.head.id.get === 10)
    }

    fragment.reset()
    assert(updated.iterator.size === 0)
    assert(missed.iterator.size === 0)
  }

  it should "compile MasterJoinUpdate operator with master selection with 1 arugment" in {
    val operator = OperatorExtractor
      .extract(classOf[MasterJoinUpdateOp], classOf[MasterJoinUpdateOperator], "updateWithSelectionWith1Argument")
      .input("hoges", ClassDescription.of(classOf[Hoge]),
        Groups.parse(Seq("id")))
      .input("foos", ClassDescription.of(classOf[Foo]),
        Groups.parse(Seq("hogeId"), Seq("+id")))
      .output("updated", ClassDescription.of(classOf[Foo]))
      .output("missed", ClassDescription.of(classOf[Foo]))
      .build()

    implicit val context = newOperatorCompilerContext("flowId")

    val thisType = OperatorCompiler.compile(operator, OperatorType.CoGroupType)
    val cls = context.loadClass[Fragment[Seq[Iterator[_]]]](thisType.getClassName)

    val updated = new GenericOutputFragment[Foo]
    val missed = new GenericOutputFragment[Foo]

    val fragment = cls
      .getConstructor(
        classOf[Map[BroadcastId, Broadcast[_]]],
        classOf[Fragment[_]], classOf[Fragment[_]])
      .newInstance(Map.empty, updated, missed)

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
      val updateds = updated.iterator.toSeq
      assert(updateds.size === 10)
      assert(updateds.map(_.id.get) === (0 until 10))
      val misseds = missed.iterator.toSeq
      assert(misseds.size === 0)
    }

    fragment.reset()
    assert(updated.iterator.size === 0)
    assert(missed.iterator.size === 0)

    {
      fragment.reset()
      val hoges = Seq.empty[Hoge]
      val foo = new Foo()
      foo.id.modify(10)
      foo.hogeId.modify(1)
      val foos = Seq(foo)
      fragment.add(Seq(hoges.iterator, foos.iterator))
      val updateds = updated.iterator.toSeq
      assert(updateds.size === 0)
      val misseds = missed.iterator.toSeq
      assert(misseds.size === 1)
      assert(misseds.head.id.get === 10)
    }

    fragment.reset()
    assert(updated.iterator.size === 0)
    assert(missed.iterator.size === 0)
  }

  it should "compile MasterJoinUpdate operator without master selection with projective model" in {
    val operator = OperatorExtractor
      .extract(classOf[MasterJoinUpdateOp], classOf[MasterJoinUpdateOperator], "updatep")
      .input("hoges", ClassDescription.of(classOf[Hoge]),
        Groups.parse(Seq("id")))
      .input("foos", ClassDescription.of(classOf[Foo]),
        Groups.parse(Seq("hogeId"), Seq("+id")))
      .output("updated", ClassDescription.of(classOf[Foo]))
      .output("missed", ClassDescription.of(classOf[Foo]))
      .build()

    implicit val context = newOperatorCompilerContext("flowId")

    val thisType = OperatorCompiler.compile(operator, OperatorType.CoGroupType)
    val cls = context.loadClass[Fragment[Seq[Iterator[_]]]](thisType.getClassName)

    val updated = new GenericOutputFragment[Foo]
    val missed = new GenericOutputFragment[Foo]

    val fragment = cls
      .getConstructor(
        classOf[Map[BroadcastId, Broadcast[_]]],
        classOf[Fragment[_]], classOf[Fragment[_]])
      .newInstance(Map.empty, updated, missed)

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
      val updateds = updated.iterator.toSeq
      assert(updateds.size === 1)
      assert(updateds.head.id.get === 10)
      val misseds = missed.iterator.toSeq
      assert(misseds.size === 0)
    }

    fragment.reset()
    assert(updated.iterator.size === 0)
    assert(missed.iterator.size === 0)

    {
      fragment.reset()
      val hoges = Seq.empty[Hoge]
      val foo = new Foo()
      foo.id.modify(10)
      foo.hogeId.modify(1)
      val foos = Seq(foo)
      fragment.add(Seq(hoges.iterator, foos.iterator))
      val updateds = updated.iterator.toSeq
      assert(updateds.size === 0)
      val misseds = missed.iterator.toSeq
      assert(misseds.size === 1)
      assert(misseds.head.id.get === 10)
    }

    fragment.reset()
    assert(updated.iterator.size === 0)
    assert(missed.iterator.size === 0)
  }

  it should "compile MasterJoinUpdate operator with master selection with projective model" in {
    val operator = OperatorExtractor
      .extract(classOf[MasterJoinUpdateOp], classOf[MasterJoinUpdateOperator], "updateWithSelectionp")
      .input("hoges", ClassDescription.of(classOf[Hoge]),
        Groups.parse(Seq("id")))
      .input("foos", ClassDescription.of(classOf[Foo]),
        Groups.parse(Seq("hogeId"), Seq("+id")))
      .output("updated", ClassDescription.of(classOf[Foo]))
      .output("missed", ClassDescription.of(classOf[Foo]))
      .build()

    implicit val context = newOperatorCompilerContext("flowId")

    val thisType = OperatorCompiler.compile(operator, OperatorType.CoGroupType)
    val cls = context.loadClass[Fragment[Seq[Iterator[_]]]](thisType.getClassName)

    val updated = new GenericOutputFragment[Foo]
    val missed = new GenericOutputFragment[Foo]

    val fragment = cls
      .getConstructor(
        classOf[Map[BroadcastId, Broadcast[_]]],
        classOf[Fragment[_]], classOf[Fragment[_]])
      .newInstance(Map.empty, updated, missed)

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
      val updateds = updated.iterator.toSeq
      assert(updateds.size === 5)
      assert(updateds.map(_.id.get) === (0 until 10 by 2))
      val misseds = missed.iterator.toSeq
      assert(misseds.size === 5)
      assert(misseds.map(_.id.get) === (1 until 10 by 2))
    }

    fragment.reset()
    assert(updated.iterator.size === 0)
    assert(missed.iterator.size === 0)

    {
      fragment.reset()
      val hoges = Seq.empty[Hoge]
      val foo = new Foo()
      foo.id.modify(10)
      foo.hogeId.modify(1)
      val foos = Seq(foo)
      fragment.add(Seq(hoges.iterator, foos.iterator))
      val updateds = updated.iterator.toSeq
      assert(updateds.size === 0)
      val misseds = missed.iterator.toSeq
      assert(misseds.size === 1)
      assert(misseds.head.id.get === 10)
    }

    fragment.reset()
    assert(updated.iterator.size === 0)
    assert(missed.iterator.size === 0)
  }
}

object ShuffledMasterJoinUpdateOperatorCompilerSpec {

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

  class MasterJoinUpdateOperator {

    @MasterJoinUpdateOp
    def update(hoge: Hoge, foo: Foo): Unit = {}

    @MasterJoinUpdateOp(selection = "select")
    def updateWithSelection(hoge: Hoge, foo: Foo): Unit = {}

    @MasterJoinUpdateOp(selection = "select1")
    def updateWithSelectionWith1Argument(hoge: Hoge, foo: Foo): Unit = {}

    @MasterSelection
    def select(hoges: JList[Hoge], foo: Foo): Hoge = {
      if (foo.id.get % 2 == 0) {
        hoges.headOption.orNull
      } else {
        null
      }
    }

    @MasterSelection
    def select1(hoges: JList[Hoge]): Hoge = {
      hoges.headOption.orNull
    }

    @MasterJoinUpdateOp
    def updatep[H <: HogeP, F <: FooP](hoge: H, foo: F): Unit = {}

    @MasterJoinUpdateOp(selection = "selectp")
    def updateWithSelectionp[H <: HogeP, F <: FooP](hoge: H, foo: F): Unit = {}

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
