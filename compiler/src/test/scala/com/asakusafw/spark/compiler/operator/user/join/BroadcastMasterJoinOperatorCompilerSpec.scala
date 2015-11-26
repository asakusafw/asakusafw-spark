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

import org.apache.hadoop.io.Writable
import org.apache.spark.broadcast.Broadcast

import com.asakusafw.lang.compiler.model.description.ClassDescription
import com.asakusafw.lang.compiler.model.graph.{ Groups, MarkerOperator }
import com.asakusafw.lang.compiler.model.testing.OperatorExtractor
import com.asakusafw.lang.compiler.planning.PlanMarker
import com.asakusafw.runtime.model.DataModel
import com.asakusafw.runtime.value.{ IntOption, StringOption }
import com.asakusafw.spark.compiler.broadcast.MockBroadcast
import com.asakusafw.spark.compiler.spi.{ OperatorCompiler, OperatorType }
import com.asakusafw.spark.runtime.fragment.{ Fragment, GenericOutputFragment }
import com.asakusafw.spark.runtime.graph.BroadcastId
import com.asakusafw.spark.runtime.io.WritableSerDe
import com.asakusafw.spark.runtime.rdd.ShuffleKey
import com.asakusafw.spark.tools.asm._
import com.asakusafw.vocabulary.model.{ Joined, Key }
import com.asakusafw.vocabulary.operator.{ MasterJoin => MasterJoinOp, MasterSelection }

@RunWith(classOf[JUnitRunner])
class BroadcastMasterJoinOperatorCompilerSpecTest extends BroadcastMasterJoinOperatorCompilerSpec

class BroadcastMasterJoinOperatorCompilerSpec extends FlatSpec with UsingCompilerContext {

  import BroadcastMasterJoinOperatorCompilerSpec._

  behavior of classOf[BroadcastMasterJoinOperatorCompiler].getSimpleName

  it should "compile MasterJoin operator without master selection" in {
    val foosMarker = MarkerOperator.builder(ClassDescription.of(classOf[Foo]))
      .attribute(classOf[PlanMarker], PlanMarker.BROADCAST).build()

    val operator = OperatorExtractor
      .extract(classOf[MasterJoinOp], classOf[MasterJoinOperator], "join")
      .input("foos", ClassDescription.of(classOf[Foo]),
        Groups.parse(Seq("id")), foosMarker.getOutput)
      .input("bars", ClassDescription.of(classOf[Bar]),
        Groups.parse(Seq("fooId"), Seq("+id")))
      .output("joined", ClassDescription.of(classOf[FooBar]))
      .output("missed", ClassDescription.of(classOf[Bar]))
      .build()

    implicit val context = newOperatorCompilerContext("flowId")

    val thisType = OperatorCompiler.compile(operator, OperatorType.ExtractType)
    context.addClass(context.broadcastIds)
    val cls = context.loadClass[Fragment[Bar]](thisType.getClassName)

    val broadcastIdsCls = context.loadClass(context.broadcastIds.thisType.getClassName)
    def getBroadcastId(marker: MarkerOperator): BroadcastId = {
      val sn = marker.getSerialNumber
      broadcastIdsCls.getField(context.broadcastIds.getField(sn)).get(null).asInstanceOf[BroadcastId]
    }

    val joined = new GenericOutputFragment[FooBar]()
    val missed = new GenericOutputFragment[Bar]()

    val ctor = cls
      .getConstructor(
        classOf[Map[BroadcastId, Broadcast[_]]],
        classOf[Fragment[_]], classOf[Fragment[_]])

    {
      val foo = new Foo()
      foo.id.modify(1)
      foo.foo.modify("foo")
      val foos = Seq(foo)
      val shuffleKey = new ShuffleKey(
        WritableSerDe.serialize(foo.id), Array.emptyByteArray)
      val fragment = ctor.newInstance(
        Map(getBroadcastId(foosMarker) -> new MockBroadcast(0, Map(shuffleKey -> foos))),
        joined,
        missed)
      fragment.reset()
      val bar = new Bar()
      bar.id.modify(10)
      bar.fooId.modify(1)
      bar.bar.modify("bar")
      val bars = Seq(bar)
      fragment.add(bar)
      val joineds = joined.iterator.toSeq
      assert(joineds.size === 1)
      assert(joineds.head.id.get === 1)
      assert(joineds.head.foo.getAsString === "foo")
      assert(joineds.head.bar.getAsString === "bar")
      val misseds = missed.iterator.toSeq
      assert(misseds.size === 0)

      fragment.reset()
      assert(joined.iterator.size === 0)
      assert(missed.iterator.size === 0)
    }

    {
      val fragment = ctor.newInstance(
        Map(getBroadcastId(foosMarker) -> new MockBroadcast(0, Map.empty)),
        joined,
        missed)
      fragment.reset()
      val bar = new Bar()
      bar.id.modify(10)
      bar.fooId.modify(1)
      bar.bar.modify("bar")
      fragment.add(bar)
      val joineds = joined.iterator.toSeq
      assert(joineds.size === 0)
      val misseds = missed.iterator.toSeq
      assert(misseds.size === 1)
      assert(misseds.head.id.get === 10)

      fragment.reset()
      assert(joined.iterator.size === 0)
      assert(missed.iterator.size === 0)
    }
  }

  it should "compile MasterJoin operator with master selection" in {
    val foosMarker = MarkerOperator.builder(ClassDescription.of(classOf[Foo]))
      .attribute(classOf[PlanMarker], PlanMarker.BROADCAST).build()

    val operator = OperatorExtractor
      .extract(classOf[MasterJoinOp], classOf[MasterJoinOperator], "joinWithSelection")
      .input("foos", ClassDescription.of(classOf[Foo]),
        Groups.parse(Seq("id")), foosMarker.getOutput)
      .input("bars", ClassDescription.of(classOf[Bar]),
        Groups.parse(Seq("fooId"), Seq("+id")))
      .output("joined", ClassDescription.of(classOf[FooBar]))
      .output("missed", ClassDescription.of(classOf[Bar]))
      .build()

    implicit val context = newOperatorCompilerContext("flowId")

    val thisType = OperatorCompiler.compile(operator, OperatorType.ExtractType)
    context.addClass(context.broadcastIds)
    val cls = context.loadClass[Fragment[Bar]](thisType.getClassName)

    val broadcastIdsCls = context.loadClass(context.broadcastIds.thisType.getClassName)
    def getBroadcastId(marker: MarkerOperator): BroadcastId = {
      val sn = marker.getSerialNumber
      broadcastIdsCls.getField(context.broadcastIds.getField(sn)).get(null).asInstanceOf[BroadcastId]
    }

    val joined = new GenericOutputFragment[FooBar]()
    val missed = new GenericOutputFragment[Bar]()

    val ctor = cls
      .getConstructor(
        classOf[Map[BroadcastId, Broadcast[_]]],
        classOf[Fragment[_]], classOf[Fragment[_]])

    {
      val foo = new Foo()
      foo.id.modify(0)
      foo.foo.modify("foo")
      val foos = Seq(foo)
      val shuffleKey = new ShuffleKey(
        WritableSerDe.serialize(foo.id), Array.emptyByteArray)
      val fragment = ctor.newInstance(
        Map(getBroadcastId(foosMarker) -> new MockBroadcast(0, Map(shuffleKey -> foos))),
        joined,
        missed)
      fragment.reset()
      (0 until 10).foreach { i =>
        val bar = new Bar()
        bar.id.modify(i)
        bar.fooId.modify(0)
        bar.bar.modify(s"bar ${i}")
        fragment.add(bar)
      }
      val joineds = joined.iterator.toSeq
      assert(joineds.size === 5)
      assert(joineds.map(_.id.get) === (0 until 10 by 2).map(_ => 0))
      assert(joineds.map(_.foo.getAsString) === (0 until 10 by 2).map(_ => "foo"))
      assert(joineds.map(_.bar.getAsString) === (0 until 10 by 2).map(i => s"bar ${i}"))
      val misseds = missed.iterator.toSeq
      assert(misseds.size === 5)
      assert(misseds.map(_.id.get) === (1 until 10 by 2))
      assert(misseds.map(_.bar.getAsString) === (1 until 10 by 2).map(i => s"bar ${i}"))

      fragment.reset()
      assert(joined.iterator.size === 0)
      assert(missed.iterator.size === 0)
    }

    {
      val fragment = ctor.newInstance(
        Map(getBroadcastId(foosMarker) -> new MockBroadcast(0, Map.empty)),
        joined,
        missed)
      fragment.reset()
      val bar = new Bar()
      bar.id.modify(10)
      bar.fooId.modify(1)
      bar.bar.modify("bar")
      fragment.add(bar)
      val joineds = joined.iterator.toSeq
      assert(joineds.size === 0)
      val misseds = missed.iterator.toSeq
      assert(misseds.size === 1)
      assert(misseds.head.id.get === 10)

      fragment.reset()
      assert(joined.iterator.size === 0)
      assert(missed.iterator.size === 0)
    }
  }

  it should "compile MasterJoin operator with master from core.empty" in {
    val operator = OperatorExtractor
      .extract(classOf[MasterJoinOp], classOf[MasterJoinOperator], "join")
      .input("foos", ClassDescription.of(classOf[Foo]),
        Groups.parse(Seq("id")))
      .input("bars", ClassDescription.of(classOf[Bar]),
        Groups.parse(Seq("fooId"), Seq("+id")))
      .output("joined", ClassDescription.of(classOf[FooBar]))
      .output("missed", ClassDescription.of(classOf[Bar]))
      .build()

    implicit val context = newOperatorCompilerContext("flowId")

    val thisType = OperatorCompiler.compile(operator, OperatorType.ExtractType)
    val cls = context.loadClass[Fragment[Bar]](thisType.getClassName)

    val joined = new GenericOutputFragment[FooBar]()
    val missed = new GenericOutputFragment[Bar]()

    val ctor = cls
      .getConstructor(
        classOf[Map[BroadcastId, Broadcast[_]]],
        classOf[Fragment[_]], classOf[Fragment[_]])

    {
      val fragment = ctor.newInstance(Map.empty, joined, missed)
      fragment.reset()
      val bar = new Bar()
      bar.id.modify(10)
      bar.fooId.modify(1)
      bar.bar.modify("bar")
      fragment.add(bar)
      val joineds = joined.iterator.toSeq
      assert(joineds.size === 0)
      val misseds = missed.iterator.toSeq
      assert(misseds.size === 1)
      assert(misseds.head.id.get === 10)

      fragment.reset()
      assert(joined.iterator.size === 0)
      assert(missed.iterator.size === 0)
    }
  }
}

object BroadcastMasterJoinOperatorCompilerSpec {

  class Foo extends DataModel[Foo] with Writable {

    val id = new IntOption()
    val foo = new StringOption()

    override def reset(): Unit = {
      id.setNull()
      foo.setNull()
    }
    override def copyFrom(other: Foo): Unit = {
      id.copyFrom(other.id)
      foo.copyFrom(other.foo)
    }
    override def readFields(in: DataInput): Unit = {
      id.readFields(in)
      foo.readFields(in)
    }
    override def write(out: DataOutput): Unit = {
      id.write(out)
      foo.write(out)
    }

    def getIdOption: IntOption = id
    def getFooOption: StringOption = foo
  }

  class Bar extends DataModel[Bar] with Writable {

    val id = new IntOption()
    val fooId = new IntOption()
    val bar = new StringOption()

    override def reset(): Unit = {
      id.setNull()
      fooId.setNull()
      bar.setNull()
    }
    override def copyFrom(other: Bar): Unit = {
      id.copyFrom(other.id)
      fooId.copyFrom(other.fooId)
      bar.copyFrom(other.bar)
    }
    override def readFields(in: DataInput): Unit = {
      id.readFields(in)
      fooId.readFields(in)
      bar.readFields(in)
    }
    override def write(out: DataOutput): Unit = {
      id.write(out)
      fooId.write(out)
      bar.write(out)
    }

    def getIdOption: IntOption = id
    def getFooIdOption: IntOption = fooId
    def getBarOption: StringOption = bar
  }

  @Joined(terms = Array(
    new Joined.Term(source = classOf[Foo], shuffle = new Key(group = Array("id")), mappings = Array(
      new Joined.Mapping(source = "id", destination = "id"),
      new Joined.Mapping(source = "foo", destination = "foo"))),
    new Joined.Term(source = classOf[Bar], shuffle = new Key(group = Array("fooId")), mappings = Array(
      new Joined.Mapping(source = "fooId", destination = "id"),
      new Joined.Mapping(source = "bar", destination = "bar")))))
  class FooBar extends DataModel[FooBar] with Writable {

    val id = new IntOption()
    val foo = new StringOption()
    val bar = new StringOption()

    override def reset(): Unit = {
      id.setNull()
      foo.setNull()
      bar.setNull()
    }
    override def copyFrom(other: FooBar): Unit = {
      id.copyFrom(other.id)
      foo.copyFrom(other.foo)
      bar.copyFrom(other.bar)
    }
    override def readFields(in: DataInput): Unit = {
      id.readFields(in)
      foo.readFields(in)
      bar.readFields(in)
    }
    override def write(out: DataOutput): Unit = {
      id.write(out)
      foo.write(out)
      bar.write(out)
    }

    def getIdOption: IntOption = id
    def getFooOption: StringOption = foo
    def getBarOption: StringOption = bar
  }

  class MasterJoinOperator {

    @MasterJoinOp
    def join(foo: Foo, bar: Bar): FooBar = ???

    @MasterJoinOp(selection = "select")
    def joinWithSelection(foo: Foo, bar: Bar): FooBar = ???

    @MasterSelection
    def select(foos: JList[Foo], bar: Bar): Foo = {
      if (bar.id.get % 2 == 0) {
        foos.headOption.orNull
      } else {
        null
      }
    }
  }
}
