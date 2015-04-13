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
import com.asakusafw.vocabulary.model.{ Joined, Key }
import com.asakusafw.vocabulary.operator.{ MasterJoin => MasterJoinOp, MasterSelection }

@RunWith(classOf[JUnitRunner])
class ShuffledMasterJoinOperatorCompilerSpecTest extends ShuffledMasterJoinOperatorCompilerSpec

class ShuffledMasterJoinOperatorCompilerSpec extends FlatSpec with LoadClassSugar {

  import ShuffledMasterJoinOperatorCompilerSpec._

  behavior of classOf[ShuffledMasterJoinOperatorCompiler].getSimpleName

  it should "compile MasterJoin operator without master selection" in {
    val operator = OperatorExtractor
      .extract(classOf[MasterJoinOp], classOf[MasterJoinOperator], "join")
      .input("hoges", ClassDescription.of(classOf[Hoge]),
        Groups.parse(Seq("id")))
      .input("foos", ClassDescription.of(classOf[Foo]),
        Groups.parse(Seq("hogeId"), Seq("+id")))
      .output("joined", ClassDescription.of(classOf[Baa]))
      .output("missed", ClassDescription.of(classOf[Foo]))
      .build()

    val classpath = Files.createTempDirectory("MasterJoinOperatorCompilerSpec").toFile
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

    val joined = new GenericOutputFragment[Baa]
    val missed = new GenericOutputFragment[Foo]

    val fragment = cls
      .getConstructor(
        classOf[Map[BroadcastId, Broadcast[_]]],
        classOf[Fragment[_]], classOf[Fragment[_]])
      .newInstance(Map.empty, joined, missed)

    {
      val hoge = new Hoge()
      hoge.id.modify(1)
      hoge.hoge.modify("hoge")
      val hoges = Seq(hoge)
      val foo = new Foo()
      foo.id.modify(10)
      foo.hogeId.modify(1)
      foo.foo.modify("foo")
      val foos = Seq(foo)
      fragment.add(Seq(hoges, foos))
      assert(joined.size === 1)
      assert(joined.head.id.get === 1)
      assert(joined.head.hoge.getAsString === "hoge")
      assert(joined.head.foo.getAsString === "foo")
      assert(missed.size === 0)
    }

    fragment.reset()
    assert(joined.size === 0)
    assert(missed.size === 0)

    {
      val hoges = Seq.empty[Hoge]
      val foo = new Foo()
      foo.id.modify(10)
      foo.hogeId.modify(1)
      foo.foo.modify("foo")
      val foos = Seq(foo)
      fragment.add(Seq(hoges, foos))
      assert(joined.size === 0)
      assert(missed.size === 1)
      assert(missed.head.id.get === 10)
    }

    fragment.reset()
    assert(joined.size === 0)
    assert(missed.size === 0)
  }

  it should "compile MasterJoin operator with master selection" in {
    val operator = OperatorExtractor
      .extract(classOf[MasterJoinOp], classOf[MasterJoinOperator], "joinWithSelection")
      .input("hoges", ClassDescription.of(classOf[Hoge]),
        Groups.parse(Seq("id")))
      .input("foos", ClassDescription.of(classOf[Foo]),
        Groups.parse(Seq("hogeId"), Seq("+id")))
      .output("joined", ClassDescription.of(classOf[Baa]))
      .output("missed", ClassDescription.of(classOf[Foo]))
      .build()

    val classpath = Files.createTempDirectory("MasterJoinOperatorCompilerSpec").toFile
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

    val joined = new GenericOutputFragment[Baa]
    val missed = new GenericOutputFragment[Foo]

    val fragment = cls
      .getConstructor(
        classOf[Map[BroadcastId, Broadcast[_]]],
        classOf[Fragment[_]], classOf[Fragment[_]])
      .newInstance(Map.empty, joined, missed)

    {
      val hoge = new Hoge()
      hoge.id.modify(0)
      hoge.hoge.modify("hoge")
      val hoges = Seq(hoge)
      val foos = (0 until 10).map { i =>
        val foo = new Foo()
        foo.id.modify(i)
        foo.hogeId.modify(0)
        foo.foo.modify(s"foo ${i}")
        foo
      }
      fragment.add(Seq(hoges, foos))
      assert(joined.size === 5)
      assert(joined.map(_.id.get) === (0 until 10 by 2).map(_ => 0))
      assert(joined.map(_.hoge.getAsString) === (0 until 10 by 2).map(_ => "hoge"))
      assert(joined.map(_.foo.getAsString) === (0 until 10 by 2).map(i => s"foo ${i}"))
      assert(missed.size === 5)
      assert(missed.map(_.id.get) === (1 until 10 by 2))
      assert(missed.map(_.foo.getAsString) === (1 until 10 by 2).map(i => s"foo ${i}"))
    }

    fragment.reset()
    assert(joined.size === 0)
    assert(missed.size === 0)

    {
      val hoges = Seq.empty[Hoge]
      val foo = new Foo()
      foo.id.modify(10)
      foo.hogeId.modify(1)
      foo.foo.modify("foo")
      val foos = Seq(foo)
      fragment.add(Seq(hoges, foos))
      assert(joined.size === 0)
      assert(missed.size === 1)
      assert(missed.head.id.get === 10)
    }

    fragment.reset()
    assert(joined.size === 0)
    assert(missed.size === 0)
  }
}

object ShuffledMasterJoinOperatorCompilerSpec {

  class Hoge extends DataModel[Hoge] {

    val id = new IntOption()
    val hoge = new StringOption()

    override def reset(): Unit = {
      id.setNull()
      hoge.setNull()
    }
    override def copyFrom(other: Hoge): Unit = {
      id.copyFrom(other.id)
      hoge.copyFrom(other.hoge)
    }

    def getIdOption: IntOption = id
    def getHogeOption: StringOption = hoge
  }

  class Foo extends DataModel[Foo] {

    val id = new IntOption()
    val hogeId = new IntOption()
    val foo = new StringOption()

    override def reset(): Unit = {
      id.setNull()
      hogeId.setNull()
      foo.setNull()
    }
    override def copyFrom(other: Foo): Unit = {
      id.copyFrom(other.id)
      hogeId.copyFrom(other.hogeId)
      foo.copyFrom(other.foo)
    }

    def getIdOption: IntOption = id
    def getHogeIdOption: IntOption = hogeId
    def getFooOption: StringOption = foo
  }

  @Joined(terms = Array(
    new Joined.Term(source = classOf[Hoge], shuffle = new Key(group = Array("id")), mappings = Array(
      new Joined.Mapping(source = "id", destination = "id"),
      new Joined.Mapping(source = "hoge", destination = "hoge"))),
    new Joined.Term(source = classOf[Foo], shuffle = new Key(group = Array("hogeId")), mappings = Array(
      new Joined.Mapping(source = "hogeId", destination = "id"),
      new Joined.Mapping(source = "foo", destination = "foo")))))
  class Baa extends DataModel[Baa] {

    val id = new IntOption()
    val hoge = new StringOption()
    val foo = new StringOption()

    override def reset(): Unit = {
      id.setNull()
      hoge.setNull()
      foo.setNull()
    }
    override def copyFrom(other: Baa): Unit = {
      id.copyFrom(other.id)
      hoge.copyFrom(other.hoge)
      foo.copyFrom(other.foo)
    }

    def getIdOption: IntOption = id
    def getHogeOption: StringOption = hoge
    def getFooOption: StringOption = foo
  }

  class MasterJoinOperator {

    @MasterJoinOp
    def join(hoge: Hoge, foo: Foo): Baa = ???

    @MasterJoinOp(selection = "select")
    def joinWithSelection(hoge: Hoge, foo: Foo): Baa = ???

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
