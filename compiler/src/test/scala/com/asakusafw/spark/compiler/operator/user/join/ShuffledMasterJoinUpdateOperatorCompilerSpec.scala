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
package user.join

import org.junit.runner.RunWith
import org.scalatest.{ Assertions, FlatSpec }
import org.scalatest.junit.JUnitRunner

import java.io.{ DataInput, DataOutput }
import java.util.{ List => JList }
import java.util.function.Consumer

import scala.collection.JavaConversions._

import org.apache.hadoop.io.Writable
import org.apache.spark.broadcast.{ Broadcast => Broadcasted }

import com.asakusafw.lang.compiler.model.description.ClassDescription
import com.asakusafw.lang.compiler.model.graph.{ Groups, MarkerOperator, Operator, OperatorInput }
import com.asakusafw.lang.compiler.model.testing.OperatorExtractor
import com.asakusafw.lang.compiler.planning.PlanMarker
import com.asakusafw.runtime.core.{ GroupView, View }
import com.asakusafw.runtime.model.DataModel
import com.asakusafw.runtime.value.IntOption
import com.asakusafw.spark.compiler.broadcast.MockBroadcast
import com.asakusafw.spark.compiler.spi.{ OperatorCompiler, OperatorType }
import com.asakusafw.spark.runtime.fragment.{ Fragment, GenericOutputFragment }
import com.asakusafw.spark.runtime.graph.BroadcastId
import com.asakusafw.spark.runtime.io.WritableSerDe
import com.asakusafw.spark.runtime.rdd.ShuffleKey
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
      .input("foos", ClassDescription.of(classOf[Foo]),
        Groups.parse(Seq("id")))
      .input("bars", ClassDescription.of(classOf[Bar]),
        Groups.parse(Seq("fooId"), Seq("+id")))
      .output("updated", ClassDescription.of(classOf[Bar]))
      .output("missed", ClassDescription.of(classOf[Bar]))
      .build()

    implicit val context = newOperatorCompilerContext("flowId")

    val thisType = OperatorCompiler.compile(operator, OperatorType.CoGroupType)
    val cls = context.loadClass[Fragment[Seq[Iterator[_]]]](thisType.getClassName)

    val updated = new GenericOutputFragment[Bar]()
    val missed = new GenericOutputFragment[Bar]()

    val fragment = cls
      .getConstructor(
        classOf[Map[BroadcastId, Broadcasted[_]]],
        classOf[Fragment[_]], classOf[Fragment[_]])
      .newInstance(Map.empty, updated, missed)

    {
      fragment.reset()
      val foo = new Foo()
      foo.id.modify(1)
      val foos = Seq(foo)
      val bar = new Bar()
      bar.id.modify(10)
      bar.fooId.modify(1)
      val bars = Seq(bar)
      fragment.add(IndexedSeq(foos.iterator, bars.iterator))
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
      val foos = Iterator.empty
      val bar = new Bar()
      bar.id.modify(10)
      bar.fooId.modify(1)
      val bars = Iterator(bar)
      fragment.add(IndexedSeq(foos, bars))
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
      .input("foos", ClassDescription.of(classOf[Foo]),
        Groups.parse(Seq("id")))
      .input("bars", ClassDescription.of(classOf[Bar]),
        Groups.parse(Seq("fooId"), Seq("+id")))
      .output("updated", ClassDescription.of(classOf[Bar]))
      .output("missed", ClassDescription.of(classOf[Bar]))
      .build()

    implicit val context = newOperatorCompilerContext("flowId")

    val thisType = OperatorCompiler.compile(operator, OperatorType.CoGroupType)
    val cls = context.loadClass[Fragment[Seq[Iterator[_]]]](thisType.getClassName)

    val updated = new GenericOutputFragment[Bar]()
    val missed = new GenericOutputFragment[Bar]()

    val fragment = cls
      .getConstructor(
        classOf[Map[BroadcastId, Broadcasted[_]]],
        classOf[Fragment[_]], classOf[Fragment[_]])
      .newInstance(Map.empty, updated, missed)

    {
      fragment.reset()
      val foo = new Foo()
      foo.id.modify(0)
      val foos = Seq(foo)
      val bars = (0 until 10).map { i =>
        val bar = new Bar()
        bar.id.modify(i)
        bar.fooId.modify(0)
        bar
      }
      fragment.add(IndexedSeq(foos.iterator, bars.iterator))
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
      val foos = Seq.empty[Foo]
      val bar = new Bar()
      bar.id.modify(10)
      bar.fooId.modify(1)
      val bars = Seq(bar)
      fragment.add(IndexedSeq(foos.iterator, bars.iterator))
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
      .input("foos", ClassDescription.of(classOf[Foo]),
        Groups.parse(Seq("id")))
      .input("bars", ClassDescription.of(classOf[Bar]),
        Groups.parse(Seq("fooId"), Seq("+id")))
      .output("updated", ClassDescription.of(classOf[Bar]))
      .output("missed", ClassDescription.of(classOf[Bar]))
      .build()

    implicit val context = newOperatorCompilerContext("flowId")

    val thisType = OperatorCompiler.compile(operator, OperatorType.CoGroupType)
    val cls = context.loadClass[Fragment[Seq[Iterator[_]]]](thisType.getClassName)

    val updated = new GenericOutputFragment[Bar]()
    val missed = new GenericOutputFragment[Bar]()

    val fragment = cls
      .getConstructor(
        classOf[Map[BroadcastId, Broadcasted[_]]],
        classOf[Fragment[_]], classOf[Fragment[_]])
      .newInstance(Map.empty, updated, missed)

    {
      fragment.reset()
      val foo = new Foo()
      foo.id.modify(0)
      val foos = Seq(foo)
      val bars = (0 until 10).map { i =>
        val bar = new Bar()
        bar.id.modify(i)
        bar.fooId.modify(0)
        bar
      }
      fragment.add(IndexedSeq(foos.iterator, bars.iterator))
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
      val foos = Seq.empty[Foo]
      val bar = new Bar()
      bar.id.modify(10)
      bar.fooId.modify(1)
      val bars = Seq(bar)
      fragment.add(IndexedSeq(foos.iterator, bars.iterator))
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
      .input("foos", ClassDescription.of(classOf[Foo]),
        Groups.parse(Seq("id")))
      .input("bars", ClassDescription.of(classOf[Bar]),
        Groups.parse(Seq("fooId"), Seq("+id")))
      .output("updated", ClassDescription.of(classOf[Bar]))
      .output("missed", ClassDescription.of(classOf[Bar]))
      .build()

    implicit val context = newOperatorCompilerContext("flowId")

    val thisType = OperatorCompiler.compile(operator, OperatorType.CoGroupType)
    val cls = context.loadClass[Fragment[Seq[Iterator[_]]]](thisType.getClassName)

    val updated = new GenericOutputFragment[Bar]()
    val missed = new GenericOutputFragment[Bar]()

    val fragment = cls
      .getConstructor(
        classOf[Map[BroadcastId, Broadcasted[_]]],
        classOf[Fragment[_]], classOf[Fragment[_]])
      .newInstance(Map.empty, updated, missed)

    {
      fragment.reset()
      val foo = new Foo()
      foo.id.modify(1)
      val foos = Seq(foo)
      val bar = new Bar()
      bar.id.modify(10)
      bar.fooId.modify(1)
      val bars = Seq(bar)
      fragment.add(IndexedSeq(foos.iterator, bars.iterator))
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
      val foos = Seq.empty[Foo]
      val bar = new Bar()
      bar.id.modify(10)
      bar.fooId.modify(1)
      val bars = Seq(bar)
      fragment.add(IndexedSeq(foos.iterator, bars.iterator))
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
      .input("foos", ClassDescription.of(classOf[Foo]),
        Groups.parse(Seq("id")))
      .input("bars", ClassDescription.of(classOf[Bar]),
        Groups.parse(Seq("fooId"), Seq("+id")))
      .output("updated", ClassDescription.of(classOf[Bar]))
      .output("missed", ClassDescription.of(classOf[Bar]))
      .build()

    implicit val context = newOperatorCompilerContext("flowId")

    val thisType = OperatorCompiler.compile(operator, OperatorType.CoGroupType)
    val cls = context.loadClass[Fragment[Seq[Iterator[_]]]](thisType.getClassName)

    val updated = new GenericOutputFragment[Bar]()
    val missed = new GenericOutputFragment[Bar]()

    val fragment = cls
      .getConstructor(
        classOf[Map[BroadcastId, Broadcasted[_]]],
        classOf[Fragment[_]], classOf[Fragment[_]])
      .newInstance(Map.empty, updated, missed)

    {
      fragment.reset()
      val foo = new Foo()
      foo.id.modify(0)
      val foos = Seq(foo)
      val bars = (0 until 10).map { i =>
        val bar = new Bar()
        bar.id.modify(i)
        bar.fooId.modify(0)
        bar
      }
      fragment.add(IndexedSeq(foos.iterator, bars.iterator))
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
      val foos = Seq.empty[Foo]
      val bar = new Bar()
      bar.id.modify(10)
      bar.fooId.modify(1)
      val bars = Seq(bar)
      fragment.add(IndexedSeq(foos.iterator, bars.iterator))
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

  it should "compile MasterJoinUpdate operator with view" in {
    val vMarker = MarkerOperator.builder(ClassDescription.of(classOf[Foo]))
      .attribute(classOf[PlanMarker], PlanMarker.BROADCAST).build()
    val gvMarker = MarkerOperator.builder(ClassDescription.of(classOf[Foo]))
      .attribute(classOf[PlanMarker], PlanMarker.BROADCAST).build()

    val operator = OperatorExtractor
      .extract(classOf[MasterJoinUpdateOp], classOf[MasterJoinUpdateOperator], "updateWithView")
      .input("foos", ClassDescription.of(classOf[Foo]),
        Groups.parse(Seq("id")))
      .input("bars", ClassDescription.of(classOf[Bar]),
        Groups.parse(Seq("fooId"), Seq("+id")))
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
              .group(Groups.parse(Seq("id"), Seq.empty))
              .upstream(gvMarker.getOutput)
          }
        })
      .output("updated", ClassDescription.of(classOf[Bar]))
      .output("missed", ClassDescription.of(classOf[Bar]))
      .build()

    implicit val context = newOperatorCompilerContext("flowId")

    val thisType = OperatorCompiler.compile(operator, OperatorType.CoGroupType)
    context.addClass(context.broadcastIds)
    val cls = context.loadClass[Fragment[Seq[Iterator[_]]]](thisType.getClassName)

    val broadcastIdsCls = context.loadClass(context.broadcastIds.thisType.getClassName)
    def getBroadcastId(marker: MarkerOperator): BroadcastId = {
      val sn = marker.getSerialNumber
      broadcastIdsCls.getField(context.broadcastIds.getField(sn)).get(null).asInstanceOf[BroadcastId]
    }

    val updated = new GenericOutputFragment[Bar]()
    val missed = new GenericOutputFragment[Bar]()

    val view = new MockBroadcast(0, Map(ShuffleKey.empty -> Seq(new Foo())))
    val groupview = new MockBroadcast(1,
      (0 until 10).map { i =>
        val foo = new Foo()
        foo.id.modify(i)
        new ShuffleKey(WritableSerDe.serialize(foo.id)) -> Seq(foo)
      }.toMap)

    val fragment = cls
      .getConstructor(
        classOf[Map[BroadcastId, Broadcasted[_]]],
        classOf[Fragment[_]], classOf[Fragment[_]])
      .newInstance(
        Map(
          getBroadcastId(vMarker) -> view,
          getBroadcastId(gvMarker) -> groupview),
        updated, missed)

    {
      fragment.reset()
      val foo = new Foo()
      foo.id.modify(0)
      val foos = Seq(foo)
      val bars = (0 until 10).map { i =>
        val bar = new Bar()
        bar.id.modify(i)
        bar.fooId.modify(0)
        bar
      }
      fragment.add(IndexedSeq(foos.iterator, bars.iterator))
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
      val foos = Seq.empty[Foo]
      val bar = new Bar()
      bar.id.modify(10)
      bar.fooId.modify(1)
      val bars = Seq(bar)
      fragment.add(IndexedSeq(foos.iterator, bars.iterator))
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

  trait FooP {
    def getIdOption: IntOption
  }

  class Foo extends DataModel[Foo] with FooP with Writable {

    val id = new IntOption()

    override def reset(): Unit = {
      id.setNull()
    }
    override def copyFrom(other: Foo): Unit = {
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

  trait BarP {
    def getIdOption: IntOption
    def getFooIdOption: IntOption
  }

  class Bar extends DataModel[Bar] with BarP with Writable {

    val id = new IntOption()
    val fooId = new IntOption()

    override def reset(): Unit = {
      id.setNull()
      fooId.setNull()
    }
    override def copyFrom(other: Bar): Unit = {
      id.copyFrom(other.id)
      fooId.copyFrom(other.fooId)
    }
    override def readFields(in: DataInput): Unit = {
      id.readFields(in)
      fooId.readFields(in)
    }
    override def write(out: DataOutput): Unit = {
      id.write(out)
      fooId.write(out)
    }

    def getIdOption: IntOption = id
    def getFooIdOption: IntOption = fooId
  }

  class MasterJoinUpdateOperator extends Assertions {

    @MasterJoinUpdateOp
    def update(foo: Foo, bar: Bar): Unit = {}

    @MasterJoinUpdateOp(selection = "select")
    def updateWithSelection(foo: Foo, bar: Bar): Unit = {}

    @MasterJoinUpdateOp(selection = "select1")
    def updateWithSelectionWith1Argument(foo: Foo, bar: Bar): Unit = {}

    @MasterSelection
    def select(foos: JList[Foo], bar: Bar): Foo = {
      if (bar.id.get % 2 == 0) {
        foos.headOption.orNull
      } else {
        null
      }
    }

    @MasterSelection
    def select1(foos: JList[Foo]): Foo = {
      foos.headOption.orNull
    }

    @MasterJoinUpdateOp
    def updatep[F <: FooP, B <: BarP](foo: F, bar: B): Unit = {}

    @MasterJoinUpdateOp(selection = "selectp")
    def updateWithSelectionp[F <: FooP, B <: BarP](foo: F, bar: B): Unit = {}

    @MasterSelection
    def selectp[F <: FooP, B <: BarP](foos: JList[F], bar: B): F = {
      if (bar.getIdOption.get % 2 == 0) {
        if (foos.size > 0) {
          foos.head
        } else {
          null.asInstanceOf[F]
        }
      } else {
        null.asInstanceOf[F]
      }
    }

    @MasterJoinUpdateOp(selection = "selectWithView")
    def updateWithView(foo: Foo, bar: Bar, v: View[Foo], gv: GroupView[Foo]): Unit = {
      val view = v.toSeq
      assert(view.size === 1)
      assert(view.head.id.isNull())
      val group = gv.find(bar.id).toSeq
      if (bar.id.get < 10) {
        assert(group.size === 1)
        assert(group.head.id.get === bar.id.get)
      } else {
        assert(group.size === 0)
      }
    }

    @MasterSelection
    def selectWithView(foos: JList[Foo], bar: Bar, v: View[Foo]): Foo = {
      val view = v.toSeq
      assert(view.size === 1)
      assert(view.head.id.isNull())
      if (bar.id.get % 2 == 0) {
        foos.headOption.orNull
      } else {
        null
      }
    }
  }
}
