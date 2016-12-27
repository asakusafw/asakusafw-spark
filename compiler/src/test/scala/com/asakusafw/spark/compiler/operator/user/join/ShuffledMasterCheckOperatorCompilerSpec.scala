/*
 * Copyright 2011-2016 Asakusa Framework Team.
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
import com.asakusafw.vocabulary.operator.{ MasterCheck => MasterCheckOp, MasterSelection }

@RunWith(classOf[JUnitRunner])
class ShuffledMasterCheckOperatorCompilerSpecTest extends ShuffledMasterCheckOperatorCompilerSpec

class ShuffledMasterCheckOperatorCompilerSpec extends FlatSpec with UsingCompilerContext {

  import ShuffledMasterCheckOperatorCompilerSpec._

  behavior of classOf[ShuffledMasterCheckOperatorCompiler].getSimpleName

  it should "compile MasterCheck operator without master selection" in {
    val operator = OperatorExtractor
      .extract(classOf[MasterCheckOp], classOf[MasterCheckOperator], "check")
      .input("foos", ClassDescription.of(classOf[Foo]),
        Groups.parse(Seq("id")))
      .input("bars", ClassDescription.of(classOf[Bar]),
        Groups.parse(Seq("fooId"), Seq("+id")))
      .output("found", ClassDescription.of(classOf[Bar]))
      .output("missed", ClassDescription.of(classOf[Bar]))
      .build()

    implicit val context = newOperatorCompilerContext("flowId")

    val thisType = OperatorCompiler.compile(operator, OperatorType.CoGroupType)
    val cls = context.loadClass[Fragment[Seq[Iterator[_]]]](thisType.getClassName)

    val found = new GenericOutputFragment[Bar]()
    val missed = new GenericOutputFragment[Bar]()

    val fragment = cls.getConstructor(
      classOf[Map[BroadcastId, Broadcasted[_]]],
      classOf[Fragment[_]], classOf[Fragment[_]])
      .newInstance(Map.empty, found, missed)

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
      val foos = Seq.empty[Foo]
      val bar = new Bar()
      bar.id.modify(10)
      bar.fooId.modify(1)
      val bars = Seq(bar)
      fragment.add(IndexedSeq(foos.iterator, bars.iterator))
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
      .input("foos", ClassDescription.of(classOf[Foo]),
        Groups.parse(Seq("id")))
      .input("bars", ClassDescription.of(classOf[Bar]),
        Groups.parse(Seq("fooId"), Seq("+id")))
      .output("found", ClassDescription.of(classOf[Bar]))
      .output("missed", ClassDescription.of(classOf[Bar]))
      .build()

    implicit val context = newOperatorCompilerContext("flowId")

    val thisType = OperatorCompiler.compile(operator, OperatorType.CoGroupType)
    val cls = context.loadClass[Fragment[Seq[Iterator[_]]]](thisType.getClassName)

    val found = new GenericOutputFragment[Bar]()
    val missed = new GenericOutputFragment[Bar]()

    val fragment = cls.getConstructor(
      classOf[Map[BroadcastId, Broadcasted[_]]],
      classOf[Fragment[_]], classOf[Fragment[_]])
      .newInstance(Map.empty, found, missed)

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
      val foos = Seq.empty[Foo]
      val bar = new Bar()
      bar.id.modify(10)
      bar.fooId.modify(1)
      val bars = Seq(bar)
      fragment.add(IndexedSeq(foos.iterator, bars.iterator))
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

  it should "compile MasterCheck operator with view" in {
    val vMarker = MarkerOperator.builder(ClassDescription.of(classOf[Foo]))
      .attribute(classOf[PlanMarker], PlanMarker.BROADCAST).build()
    val gvMarker = MarkerOperator.builder(ClassDescription.of(classOf[Foo]))
      .attribute(classOf[PlanMarker], PlanMarker.BROADCAST).build()

    val operator = OperatorExtractor
      .extract(classOf[MasterCheckOp], classOf[MasterCheckOperator], "checkWithView")
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
      .output("found", ClassDescription.of(classOf[Bar]))
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

    val found = new GenericOutputFragment[Bar]()
    val missed = new GenericOutputFragment[Bar]()

    val view = new MockBroadcast(0, Map(ShuffleKey.empty -> Seq(new Foo())))
    val groupview = new MockBroadcast(1,
      (0 until 10).map { i =>
        val foo = new Foo()
        foo.id.modify(i)
        new ShuffleKey(WritableSerDe.serialize(foo.id)) -> Seq(foo)
      }.toMap)

    val fragment = cls.getConstructor(
      classOf[Map[BroadcastId, Broadcasted[_]]],
      classOf[Fragment[_]], classOf[Fragment[_]])
      .newInstance(
        Map(
          getBroadcastId(vMarker) -> view,
          getBroadcastId(gvMarker) -> groupview),
        found, missed)

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
      val foos = Seq.empty[Foo]
      val bar = new Bar()
      bar.id.modify(10)
      bar.fooId.modify(1)
      val bars = Seq(bar)
      fragment.add(IndexedSeq(foos.iterator, bars.iterator))
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

  class Foo extends DataModel[Foo] with Writable {

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

  class Bar extends DataModel[Bar] with Writable {

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

  class MasterCheckOperator extends Assertions {

    @MasterCheckOp
    def check(foo: Foo, bar: Bar): Boolean = ???

    @MasterCheckOp(selection = "select")
    def checkWithSelection(foo: Foo, bar: Bar): Boolean = ???

    @MasterSelection
    def select(foos: JList[Foo], bar: Bar): Foo = {
      if (bar.id.get % 2 == 0) {
        foos.headOption.orNull
      } else {
        null
      }
    }

    @MasterCheckOp(selection = "selectWithView")
    def checkWithView(foo: Foo, bar: Bar, v: View[Foo], gv: GroupView[Foo]): Boolean = ???

    @MasterSelection
    def selectWithView(foos: JList[Foo], bar: Bar, v: View[Foo], gv: GroupView[Foo]): Foo = {
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
      if (bar.id.get % 2 == 0) {
        foos.headOption.orNull
      } else {
        null
      }
    }
  }
}
