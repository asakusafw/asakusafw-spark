package com.asakusafw.spark.compiler.operator
package user.join

import org.junit.runner.RunWith
import org.scalatest.FlatSpec
import org.scalatest.junit.JUnitRunner

import java.nio.file.Files
import java.util.{ List => JList }

import scala.collection.JavaConversions._
import scala.collection.mutable

import org.apache.spark.broadcast.Broadcast

import com.asakusafw.lang.compiler.api.CompilerOptions
import com.asakusafw.lang.compiler.api.testing.MockJobflowProcessorContext
import com.asakusafw.lang.compiler.model.PropertyName
import com.asakusafw.lang.compiler.model.description._
import com.asakusafw.lang.compiler.model.graph.Group
import com.asakusafw.lang.compiler.model.testing.OperatorExtractor
import com.asakusafw.runtime.core.Result
import com.asakusafw.runtime.model.DataModel
import com.asakusafw.runtime.value._
import com.asakusafw.spark.compiler.spi.{ OperatorCompiler, OperatorType }
import com.asakusafw.spark.compiler.subplan.{ BranchKeysClassBuilder, BroadcastIdsClassBuilder }
import com.asakusafw.spark.runtime.driver.BroadcastId
import com.asakusafw.spark.runtime.fragment._
import com.asakusafw.spark.tools.asm._
import com.asakusafw.vocabulary.operator.{ MasterJoinUpdate => MasterJoinUpdateOp, MasterSelection }

@RunWith(classOf[JUnitRunner])
class ShuffledMasterJoinUpdateOperatorCompilerSpecTest extends ShuffledMasterJoinUpdateOperatorCompilerSpec

class ShuffledMasterJoinUpdateOperatorCompilerSpec extends FlatSpec with LoadClassSugar {

  import ShuffledMasterJoinUpdateOperatorCompilerSpec._

  behavior of classOf[ShuffledMasterJoinUpdateOperatorCompiler].getSimpleName

  it should "compile MasterJoinUpdate operator without master selection" in {
    val operator = OperatorExtractor
      .extract(classOf[MasterJoinUpdateOp], classOf[MasterJoinUpdateOperator], "update")
      .input("hoges", ClassDescription.of(classOf[Hoge]),
        new Group(Seq(PropertyName.of("id")), Seq.empty[Group.Ordering]))
      .input("foos", ClassDescription.of(classOf[Foo]),
        new Group(
          Seq(PropertyName.of("hogeId")),
          Seq(new Group.Ordering(PropertyName.of("id"), Group.Direction.ASCENDANT))))
      .output("updated", ClassDescription.of(classOf[Foo]))
      .output("missed", ClassDescription.of(classOf[Foo]))
      .build()

    val classpath = Files.createTempDirectory("MasterJoinUpdateOperatorCompilerSpec").toFile
    implicit val context = OperatorCompiler.Context(
      flowId = "flowId",
      jpContext = new MockJobflowProcessorContext(
        new CompilerOptions("buildid", "", Map.empty[String, String]),
        Thread.currentThread.getContextClassLoader,
        classpath),
      branchKeys = new BranchKeysClassBuilder("flowId"),
      broadcastIds = new BroadcastIdsClassBuilder("flowId"),
      shuffleKeyTypes = mutable.Set.empty)

    val thisType = OperatorCompiler.compile(operator, OperatorType.CoGroupType)
    val cls = loadClass(thisType.getClassName, classpath).asSubclass(classOf[Fragment[Seq[Iterable[_]]]])

    val updated = new GenericOutputFragment[Foo]
    val missed = new GenericOutputFragment[Foo]

    val fragment = cls
      .getConstructor(
        classOf[Map[BroadcastId, Broadcast[_]]],
        classOf[Fragment[_]], classOf[Fragment[_]])
      .newInstance(Map.empty, updated, missed)

    {
      val hoge = new Hoge()
      hoge.id.modify(1)
      val hoges = Seq(hoge)
      val foo = new Foo()
      foo.id.modify(10)
      foo.hogeId.modify(1)
      val foos = Seq(foo)
      fragment.add(Seq(hoges, foos))
      assert(updated.size === 1)
      assert(updated.head.id.get === 10)
      assert(missed.size === 0)
    }

    fragment.reset()
    assert(updated.size === 0)
    assert(missed.size === 0)

    {
      val hoges = Seq.empty[Hoge]
      val foo = new Foo()
      foo.id.modify(10)
      foo.hogeId.modify(1)
      val foos = Seq(foo)
      fragment.add(Seq(hoges, foos))
      assert(updated.size === 0)
      assert(missed.size === 1)
      assert(missed.head.id.get === 10)
    }

    fragment.reset()
    assert(updated.size === 0)
    assert(missed.size === 0)
  }

  it should "compile MasterJoinUpdate operator with master selection" in {
    val operator = OperatorExtractor
      .extract(classOf[MasterJoinUpdateOp], classOf[MasterJoinUpdateOperator], "updateWithSelection")
      .input("hoges", ClassDescription.of(classOf[Hoge]),
        new Group(Seq(PropertyName.of("id")), Seq.empty[Group.Ordering]))
      .input("foos", ClassDescription.of(classOf[Foo]),
        new Group(
          Seq(PropertyName.of("hogeId")),
          Seq(new Group.Ordering(PropertyName.of("id"), Group.Direction.ASCENDANT))))
      .output("updated", ClassDescription.of(classOf[Foo]))
      .output("missed", ClassDescription.of(classOf[Foo]))
      .build()

    val classpath = Files.createTempDirectory("MasterJoinUpdateOperatorCompilerSpec").toFile
    implicit val context = OperatorCompiler.Context(
      flowId = "flowId",
      jpContext = new MockJobflowProcessorContext(
        new CompilerOptions("buildid", "", Map.empty[String, String]),
        Thread.currentThread.getContextClassLoader,
        classpath),
      branchKeys = new BranchKeysClassBuilder("flowId"),
      broadcastIds = new BroadcastIdsClassBuilder("flowId"),
      shuffleKeyTypes = mutable.Set.empty)

    val thisType = OperatorCompiler.compile(operator, OperatorType.CoGroupType)
    val cls = loadClass(thisType.getClassName, classpath).asSubclass(classOf[Fragment[Seq[Iterable[_]]]])

    val updated = new GenericOutputFragment[Foo]
    val missed = new GenericOutputFragment[Foo]

    val fragment = cls
      .getConstructor(
        classOf[Map[BroadcastId, Broadcast[_]]],
        classOf[Fragment[_]], classOf[Fragment[_]])
      .newInstance(Map.empty, updated, missed)

    {
      val hoge = new Hoge()
      hoge.id.modify(0)
      val hoges = Seq(hoge)
      val foos = (0 until 10).map { i =>
        val foo = new Foo()
        foo.id.modify(i)
        foo.hogeId.modify(0)
        foo
      }
      fragment.add(Seq(hoges, foos))
      assert(updated.size === 5)
      assert(updated.map(_.id.get) === (0 until 10 by 2))
      assert(missed.size === 5)
      assert(missed.map(_.id.get) === (1 until 10 by 2))
    }

    fragment.reset()
    assert(updated.size === 0)
    assert(missed.size === 0)

    {
      val hoges = Seq.empty[Hoge]
      val foo = new Foo()
      foo.id.modify(10)
      foo.hogeId.modify(1)
      val foos = Seq(foo)
      fragment.add(Seq(hoges, foos))
      assert(updated.size === 0)
      assert(missed.size === 1)
      assert(missed.head.id.get === 10)
    }

    fragment.reset()
    assert(updated.size === 0)
    assert(missed.size === 0)
  }

  it should "compile MasterJoinUpdate operator without master selection with projective model" in {
    val operator = OperatorExtractor
      .extract(classOf[MasterJoinUpdateOp], classOf[MasterJoinUpdateOperator], "updatep")
      .input("hoges", ClassDescription.of(classOf[Hoge]),
        new Group(Seq(PropertyName.of("id")), Seq.empty[Group.Ordering]))
      .input("foos", ClassDescription.of(classOf[Foo]),
        new Group(
          Seq(PropertyName.of("hogeId")),
          Seq(new Group.Ordering(PropertyName.of("id"), Group.Direction.ASCENDANT))))
      .output("updated", ClassDescription.of(classOf[Foo]))
      .output("missed", ClassDescription.of(classOf[Foo]))
      .build()

    val classpath = Files.createTempDirectory("MasterJoinUpdateOperatorCompilerSpec").toFile
    implicit val context = OperatorCompiler.Context(
      flowId = "flowId",
      jpContext = new MockJobflowProcessorContext(
        new CompilerOptions("buildid", "", Map.empty[String, String]),
        Thread.currentThread.getContextClassLoader,
        classpath),
      branchKeys = new BranchKeysClassBuilder("flowId"),
      broadcastIds = new BroadcastIdsClassBuilder("flowId"),
      shuffleKeyTypes = mutable.Set.empty)

    val thisType = OperatorCompiler.compile(operator, OperatorType.CoGroupType)
    val cls = loadClass(thisType.getClassName, classpath).asSubclass(classOf[Fragment[Seq[Iterable[_]]]])

    val updated = new GenericOutputFragment[Foo]
    val missed = new GenericOutputFragment[Foo]

    val fragment = cls
      .getConstructor(
        classOf[Map[BroadcastId, Broadcast[_]]],
        classOf[Fragment[_]], classOf[Fragment[_]])
      .newInstance(Map.empty, updated, missed)

    {
      val hoge = new Hoge()
      hoge.id.modify(1)
      val hoges = Seq(hoge)
      val foo = new Foo()
      foo.id.modify(10)
      foo.hogeId.modify(1)
      val foos = Seq(foo)
      fragment.add(Seq(hoges, foos))
      assert(updated.size === 1)
      assert(updated.head.id.get === 10)
      assert(missed.size === 0)
    }

    fragment.reset()
    assert(updated.size === 0)
    assert(missed.size === 0)

    {
      val hoges = Seq.empty[Hoge]
      val foo = new Foo()
      foo.id.modify(10)
      foo.hogeId.modify(1)
      val foos = Seq(foo)
      fragment.add(Seq(hoges, foos))
      assert(updated.size === 0)
      assert(missed.size === 1)
      assert(missed.head.id.get === 10)
    }

    fragment.reset()
    assert(updated.size === 0)
    assert(missed.size === 0)
  }

  it should "compile MasterJoinUpdate operator with master selection with projective model" in {
    val operator = OperatorExtractor
      .extract(classOf[MasterJoinUpdateOp], classOf[MasterJoinUpdateOperator], "updateWithSelectionp")
      .input("hoges", ClassDescription.of(classOf[Hoge]),
        new Group(Seq(PropertyName.of("id")), Seq.empty[Group.Ordering]))
      .input("foos", ClassDescription.of(classOf[Foo]),
        new Group(
          Seq(PropertyName.of("hogeId")),
          Seq(new Group.Ordering(PropertyName.of("id"), Group.Direction.ASCENDANT))))
      .output("updated", ClassDescription.of(classOf[Foo]))
      .output("missed", ClassDescription.of(classOf[Foo]))
      .build()

    val classpath = Files.createTempDirectory("MasterJoinUpdateOperatorCompilerSpec").toFile
    implicit val context = OperatorCompiler.Context(
      flowId = "flowId",
      jpContext = new MockJobflowProcessorContext(
        new CompilerOptions("buildid", "", Map.empty[String, String]),
        Thread.currentThread.getContextClassLoader,
        classpath),
      branchKeys = new BranchKeysClassBuilder("flowId"),
      broadcastIds = new BroadcastIdsClassBuilder("flowId"),
      shuffleKeyTypes = mutable.Set.empty)

    val thisType = OperatorCompiler.compile(operator, OperatorType.CoGroupType)
    val cls = loadClass(thisType.getClassName, classpath).asSubclass(classOf[Fragment[Seq[Iterable[_]]]])

    val updated = new GenericOutputFragment[Foo]
    val missed = new GenericOutputFragment[Foo]

    val fragment = cls
      .getConstructor(
        classOf[Map[BroadcastId, Broadcast[_]]],
        classOf[Fragment[_]], classOf[Fragment[_]])
      .newInstance(Map.empty, updated, missed)

    {
      val hoge = new Hoge()
      hoge.id.modify(0)
      val hoges = Seq(hoge)
      val foos = (0 until 10).map { i =>
        val foo = new Foo()
        foo.id.modify(i)
        foo.hogeId.modify(0)
        foo
      }
      fragment.add(Seq(hoges, foos))
      assert(updated.size === 5)
      assert(updated.map(_.id.get) === (0 until 10 by 2))
      assert(missed.size === 5)
      assert(missed.map(_.id.get) === (1 until 10 by 2))
    }

    fragment.reset()
    assert(updated.size === 0)
    assert(missed.size === 0)

    {
      val hoges = Seq.empty[Hoge]
      val foo = new Foo()
      foo.id.modify(10)
      foo.hogeId.modify(1)
      val foos = Seq(foo)
      fragment.add(Seq(hoges, foos))
      assert(updated.size === 0)
      assert(missed.size === 1)
      assert(missed.head.id.get === 10)
    }

    fragment.reset()
    assert(updated.size === 0)
    assert(missed.size === 0)
  }
}

object ShuffledMasterJoinUpdateOperatorCompilerSpec {

  trait HogeP {
    def getIdOption: IntOption
  }

  class Hoge extends DataModel[Hoge] with HogeP {

    val id = new IntOption()

    override def reset(): Unit = {
      id.setNull()
    }
    override def copyFrom(other: Hoge): Unit = {
      id.copyFrom(other.id)
    }

    def getIdOption: IntOption = id
  }

  trait FooP {
    def getIdOption: IntOption
    def getHogeIdOption: IntOption
  }

  class Foo extends DataModel[Foo] with FooP {

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

    def getIdOption: IntOption = id
    def getHogeIdOption: IntOption = hogeId
  }

  class MasterJoinUpdateOperator {

    @MasterJoinUpdateOp
    def update(hoge: Hoge, foo: Foo): Unit = {}

    @MasterJoinUpdateOp(selection = "select")
    def updateWithSelection(hoge: Hoge, foo: Foo): Unit = {}

    @MasterSelection
    def select(hoges: JList[Hoge], foo: Foo): Hoge = {
      if (foo.id.get % 2 == 0) {
        hoges.headOption.orNull
      } else {
        null
      }
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
