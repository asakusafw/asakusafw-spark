package com.asakusafw.spark.compiler.operator
package user
package join

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
import com.asakusafw.spark.runtime.fragment._
import com.asakusafw.spark.tools.asm._
import com.asakusafw.vocabulary.operator.{ MasterBranch => MasterBranchOp, MasterSelection }

@RunWith(classOf[JUnitRunner])
class ShuffledMasterBranchOperatorCompilerSpecTest extends ShuffledMasterBranchOperatorCompilerSpec

class ShuffledMasterBranchOperatorCompilerSpec extends FlatSpec with LoadClassSugar {

  import ShuffledMasterBranchOperatorCompilerSpec._

  behavior of classOf[ShuffledMasterBranchOperatorCompiler].getSimpleName

  it should "compile MasterBranch operator without master selection" in {
    val operator = OperatorExtractor
      .extract(classOf[MasterBranchOp], classOf[MasterBranchOperator], "branch")
      .input("hoges", ClassDescription.of(classOf[Hoge]),
        new Group(Seq(PropertyName.of("id")), Seq.empty[Group.Ordering]))
      .input("foos", ClassDescription.of(classOf[Foo]),
        new Group(
          Seq(PropertyName.of("hogeId")),
          Seq(new Group.Ordering(PropertyName.of("id"), Group.Direction.ASCENDANT))))
      .output("low", ClassDescription.of(classOf[Foo]))
      .output("high", ClassDescription.of(classOf[Foo]))
      .build()

    val classpath = Files.createTempDirectory("MasterBranchOperatorCompilerSpec").toFile
    implicit val context = OperatorCompiler.Context(
      flowId = "flowId",
      jpContext = new MockJobflowProcessorContext(
        new CompilerOptions("buildid", "", Map.empty[String, String]),
        Thread.currentThread.getContextClassLoader,
        classpath),
      shuffleKeyTypes = mutable.Set.empty)

    val thisType = OperatorCompiler.compile(operator, OperatorType.CoGroupType)
    val cls = loadClass(thisType.getClassName, classpath).asSubclass(classOf[Fragment[Seq[Iterable[_]]]])

    val (low, high) = {
      val builder = new OutputFragmentClassBuilder(context.flowId, classOf[Foo].asType)
      val cls = loadClass(builder.thisType.getClassName, builder.build()).asSubclass(classOf[OutputFragment[Foo]])
      (cls.newInstance(), cls.newInstance())
    }

    val fragment = cls.getConstructor(
      classOf[Map[Long, Broadcast[_]]],
      classOf[Fragment[_]], classOf[Fragment[_]])
      .newInstance(Map.empty, low, high)

    {
      val hoge = new Hoge()
      hoge.id.modify(10)
      val hoges = Seq(hoge)
      val foo = new Foo()
      foo.id.modify(10)
      foo.hogeId.modify(10)
      val foos = Seq(foo)
      fragment.add(Seq(hoges, foos))
      assert(low.buffer.size === 0)
      assert(high.buffer.size === 1)
      assert(high.buffer(0).id.get === 10)
    }

    fragment.reset()
    assert(low.buffer.size === 0)
    assert(high.buffer.size === 0)

    {
      val hoges = Seq.empty[Hoge]
      val foo = new Foo()
      foo.id.modify(10)
      foo.hogeId.modify(1)
      val foos = Seq(foo)
      fragment.add(Seq(hoges, foos))
      assert(low.buffer.size === 1)
      assert(low.buffer(0).id.get === 10)
      assert(high.buffer.size === 0)
    }

    fragment.reset()
    assert(low.buffer.size === 0)
    assert(high.buffer.size === 0)
  }

  it should "compile MasterBranch operator with master selection" in {
    val operator = OperatorExtractor
      .extract(classOf[MasterBranchOp], classOf[MasterBranchOperator], "branchWithSelection")
      .input("hoges", ClassDescription.of(classOf[Hoge]),
        new Group(Seq(PropertyName.of("id")), Seq.empty[Group.Ordering]))
      .input("foos", ClassDescription.of(classOf[Foo]),
        new Group(
          Seq(PropertyName.of("hogeId")),
          Seq(new Group.Ordering(PropertyName.of("id"), Group.Direction.ASCENDANT))))
      .output("low", ClassDescription.of(classOf[Foo]))
      .output("high", ClassDescription.of(classOf[Foo]))
      .build()

    val classpath = Files.createTempDirectory("MasterBranchOperatorCompilerSpec").toFile
    implicit val context = OperatorCompiler.Context(
      flowId = "flowId",
      jpContext = new MockJobflowProcessorContext(
        new CompilerOptions("buildid", "", Map.empty[String, String]),
        Thread.currentThread.getContextClassLoader,
        classpath),
      shuffleKeyTypes = mutable.Set.empty)

    val thisType = OperatorCompiler.compile(operator, OperatorType.CoGroupType)
    val cls = loadClass(thisType.getClassName, classpath).asSubclass(classOf[Fragment[Seq[Iterable[_]]]])

    val (low, high) = {
      val builder = new OutputFragmentClassBuilder(context.flowId, classOf[Foo].asType)
      val cls = loadClass(builder.thisType.getClassName, builder.build()).asSubclass(classOf[OutputFragment[Foo]])
      (cls.newInstance(), cls.newInstance())
    }

    val fragment = cls.getConstructor(
      classOf[Map[Long, Broadcast[_]]],
      classOf[Fragment[_]], classOf[Fragment[_]])
      .newInstance(Map.empty, low, high)

    {
      val hoge = new Hoge()
      hoge.id.modify(10)
      val hoges = Seq(hoge)
      val foos = (0 until 10).map { i =>
        val foo = new Foo()
        foo.id.modify(i)
        foo.hogeId.modify(10)
        foo
      }
      fragment.add(Seq(hoges, foos))
      assert(low.buffer.size === 5)
      assert(low.buffer.map(_.id.get) === (1 until 10 by 2))
      assert(high.buffer.size === 5)
      assert(high.buffer.map(_.id.get) === (0 until 10 by 2))
    }

    fragment.reset()
    assert(low.buffer.size === 0)
    assert(high.buffer.size === 0)

    {
      val hoges = Seq.empty[Hoge]
      val foo = new Foo()
      foo.id.modify(10)
      foo.hogeId.modify(1)
      val foos = Seq(foo)
      fragment.add(Seq(hoges, foos))
      assert(low.buffer.size === 1)
      assert(low.buffer(0).id.get === 10)
      assert(high.buffer.size === 0)
    }

    fragment.reset()
    assert(low.buffer.size === 0)
    assert(high.buffer.size === 0)
  }

  it should "compile MasterBranch operator without master selection with projective model" in {
    val operator = OperatorExtractor
      .extract(classOf[MasterBranchOp], classOf[MasterBranchOperator], "branchp")
      .input("hoges", ClassDescription.of(classOf[Hoge]),
        new Group(Seq(PropertyName.of("id")), Seq.empty[Group.Ordering]))
      .input("foos", ClassDescription.of(classOf[Foo]),
        new Group(
          Seq(PropertyName.of("hogeId")),
          Seq(new Group.Ordering(PropertyName.of("id"), Group.Direction.ASCENDANT))))
      .output("low", ClassDescription.of(classOf[Foo]))
      .output("high", ClassDescription.of(classOf[Foo]))
      .build()

    val classpath = Files.createTempDirectory("MasterBranchOperatorCompilerSpec").toFile
    implicit val context = OperatorCompiler.Context(
      flowId = "flowId",
      jpContext = new MockJobflowProcessorContext(
        new CompilerOptions("buildid", "", Map.empty[String, String]),
        Thread.currentThread.getContextClassLoader,
        classpath),
      shuffleKeyTypes = mutable.Set.empty)

    val thisType = OperatorCompiler.compile(operator, OperatorType.CoGroupType)
    val cls = loadClass(thisType.getClassName, classpath).asSubclass(classOf[Fragment[Seq[Iterable[_]]]])

    val (low, high) = {
      val builder = new OutputFragmentClassBuilder(context.flowId, classOf[Foo].asType)
      val cls = loadClass(builder.thisType.getClassName, builder.build()).asSubclass(classOf[OutputFragment[Foo]])
      (cls.newInstance(), cls.newInstance())
    }

    val fragment = cls.getConstructor(
      classOf[Map[Long, Broadcast[_]]],
      classOf[Fragment[_]], classOf[Fragment[_]])
      .newInstance(Map.empty, low, high)

    {
      val hoge = new Hoge()
      hoge.id.modify(10)
      val hoges = Seq(hoge)
      val foo = new Foo()
      foo.id.modify(10)
      foo.hogeId.modify(10)
      val foos = Seq(foo)
      fragment.add(Seq(hoges, foos))
      assert(low.buffer.size === 0)
      assert(high.buffer.size === 1)
      assert(high.buffer(0).id.get === 10)
    }

    fragment.reset()
    assert(low.buffer.size === 0)
    assert(high.buffer.size === 0)

    {
      val hoges = Seq.empty[Hoge]
      val foo = new Foo()
      foo.id.modify(10)
      foo.hogeId.modify(1)
      val foos = Seq(foo)
      fragment.add(Seq(hoges, foos))
      assert(low.buffer.size === 1)
      assert(low.buffer(0).id.get === 10)
      assert(high.buffer.size === 0)
    }

    fragment.reset()
    assert(low.buffer.size === 0)
    assert(high.buffer.size === 0)
  }

  it should "compile MasterBranch operator with master selection with projective model" in {
    val operator = OperatorExtractor
      .extract(classOf[MasterBranchOp], classOf[MasterBranchOperator], "branchWithSelectionp")
      .input("hoges", ClassDescription.of(classOf[Hoge]),
        new Group(Seq(PropertyName.of("id")), Seq.empty[Group.Ordering]))
      .input("foos", ClassDescription.of(classOf[Foo]),
        new Group(
          Seq(PropertyName.of("hogeId")),
          Seq(new Group.Ordering(PropertyName.of("id"), Group.Direction.ASCENDANT))))
      .output("low", ClassDescription.of(classOf[Foo]))
      .output("high", ClassDescription.of(classOf[Foo]))
      .build()

    val classpath = Files.createTempDirectory("MasterBranchOperatorCompilerSpec").toFile
    implicit val context = OperatorCompiler.Context(
      flowId = "flowId",
      jpContext = new MockJobflowProcessorContext(
        new CompilerOptions("buildid", "", Map.empty[String, String]),
        Thread.currentThread.getContextClassLoader,
        classpath),
      shuffleKeyTypes = mutable.Set.empty)

    val thisType = OperatorCompiler.compile(operator, OperatorType.CoGroupType)
    val cls = loadClass(thisType.getClassName, classpath).asSubclass(classOf[Fragment[Seq[Iterable[_]]]])

    val (low, high) = {
      val builder = new OutputFragmentClassBuilder(context.flowId, classOf[Foo].asType)
      val cls = loadClass(builder.thisType.getClassName, builder.build()).asSubclass(classOf[OutputFragment[Foo]])
      (cls.newInstance(), cls.newInstance())
    }

    val fragment = cls.getConstructor(
      classOf[Map[Long, Broadcast[_]]],
      classOf[Fragment[_]], classOf[Fragment[_]])
      .newInstance(Map.empty, low, high)

    {
      val hoge = new Hoge()
      hoge.id.modify(10)
      val hoges = Seq(hoge)
      val foos = (0 until 10).map { i =>
        val foo = new Foo()
        foo.id.modify(i)
        foo.hogeId.modify(10)
        foo
      }
      fragment.add(Seq(hoges, foos))
      assert(low.buffer.size === 5)
      assert(low.buffer.map(_.id.get) === (1 until 10 by 2))
      assert(high.buffer.size === 5)
      assert(high.buffer.map(_.id.get) === (0 until 10 by 2))
    }

    fragment.reset()
    assert(low.buffer.size === 0)
    assert(high.buffer.size === 0)

    {
      val hoges = Seq.empty[Hoge]
      val foo = new Foo()
      foo.id.modify(10)
      foo.hogeId.modify(1)
      val foos = Seq(foo)
      fragment.add(Seq(hoges, foos))
      assert(low.buffer.size === 1)
      assert(low.buffer(0).id.get === 10)
      assert(high.buffer.size === 0)
    }

    fragment.reset()
    assert(low.buffer.size === 0)
    assert(high.buffer.size === 0)
  }
}

object ShuffledMasterBranchOperatorCompilerSpec {

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
