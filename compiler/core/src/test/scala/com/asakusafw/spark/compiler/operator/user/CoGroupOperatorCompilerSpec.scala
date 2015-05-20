package com.asakusafw.spark.compiler
package operator
package user

import org.junit.runner.RunWith
import org.scalatest.FlatSpec
import org.scalatest.junit.JUnitRunner

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
import com.asakusafw.vocabulary.operator.CoGroup

@RunWith(classOf[JUnitRunner])
class CoGroupOperatorCompilerSpecTest extends CoGroupOperatorCompilerSpec

class CoGroupOperatorCompilerSpec extends FlatSpec with LoadClassSugar with TempDir {

  import CoGroupOperatorCompilerSpec._

  behavior of classOf[CoGroupOperatorCompiler].getSimpleName

  it should "compile CoGroup operator" in {
    val operator = OperatorExtractor
      .extract(classOf[CoGroup], classOf[CoGroupOperator], "cogroup")
      .input("hogeList", ClassDescription.of(classOf[Hoge]),
        Groups.parse(Seq("id")))
      .input("fooList", ClassDescription.of(classOf[Foo]),
        Groups.parse(Seq("hogeId"), Seq("+id")))
      .output("hogeResult", ClassDescription.of(classOf[Hoge]))
      .output("fooResult", ClassDescription.of(classOf[Foo]))
      .output("hogeError", ClassDescription.of(classOf[Hoge]))
      .output("fooError", ClassDescription.of(classOf[Foo]))
      .output("nResult", ClassDescription.of(classOf[N]))
      .argument("n", ImmediateDescription.of(10))
      .build()

    val classpath = createTempDirectory("CoGroupOperatorCompilerSpec").toFile
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
    val cls = loadClass(thisType.getClassName, classpath).asSubclass(classOf[Fragment[Seq[Iterator[_]]]])

    val hogeResult = new GenericOutputFragment[Hoge]
    val hogeError = new GenericOutputFragment[Hoge]

    val fooResult = new GenericOutputFragment[Foo]
    val fooError = new GenericOutputFragment[Foo]

    val nResult = new GenericOutputFragment[N]

    val fragment = cls.getConstructor(
      classOf[Map[BroadcastId, Broadcast[_]]],
      classOf[Fragment[_]], classOf[Fragment[_]],
      classOf[Fragment[_]], classOf[Fragment[_]],
      classOf[Fragment[_]])
      .newInstance(Map.empty, hogeResult, fooResult, hogeError, fooError, nResult)

    {
      val hoges = Seq.empty[Hoge]
      val foos = Seq.empty[Foo]
      fragment.add(Seq(hoges.iterator, foos.iterator))
      assert(hogeResult.size === 0)
      assert(fooResult.size === 0)
      assert(hogeError.size === 0)
      assert(fooError.size === 0)
      assert(nResult.size === 1)
      assert(nResult.head.n.get === 10)
    }

    fragment.reset()
    assert(hogeResult.size === 0)
    assert(fooResult.size === 0)
    assert(hogeError.size === 0)
    assert(fooError.size === 0)
    assert(nResult.size === 0)

    {
      val hoge = new Hoge()
      hoge.id.modify(1)
      val hoges = Seq(hoge)
      val foo = new Foo()
      foo.id.modify(10)
      foo.hogeId.modify(1)
      val foos = Seq(foo)
      fragment.add(Seq(hoges.iterator, foos.iterator))
      assert(hogeResult.size === 1)
      assert(hogeResult.head.id.get === hoge.id.get)
      assert(fooResult.size === 1)
      assert(fooResult.head.id.get === foo.id.get)
      assert(fooResult.head.hogeId.get === foo.hogeId.get)
      assert(hogeError.size === 0)
      assert(fooError.size === 0)
      assert(nResult.size === 1)
      assert(nResult.head.n.get === 10)
    }

    fragment.reset()
    assert(hogeResult.size === 0)
    assert(fooResult.size === 0)
    assert(hogeError.size === 0)
    assert(fooError.size === 0)
    assert(nResult.size === 0)

    {
      val hoge = new Hoge()
      hoge.id.modify(1)
      val hoges = Seq(hoge)
      val foos = (0 until 10).map { i =>
        val foo = new Foo()
        foo.id.modify(i)
        foo.hogeId.modify(1)
        foo
      }
      fragment.add(Seq(hoges.iterator, foos.iterator))
      assert(hogeResult.size === 0)
      assert(fooResult.size === 0)
      assert(hogeError.size === 1)
      assert(hogeError.head.id.get === hoge.id.get)
      assert(fooError.size === 10)
      fooError.zip(foos).foreach {
        case (actual, expected) =>
          assert(actual.id.get === expected.id.get)
          assert(actual.hogeId.get === expected.hogeId.get)
      }
      assert(nResult.size === 1)
      assert(nResult.head.n.get === 10)
    }

    fragment.reset()
    assert(hogeResult.size === 0)
    assert(fooResult.size === 0)
    assert(hogeError.size === 0)
    assert(fooError.size === 0)
    assert(nResult.size === 0)
  }
}

object CoGroupOperatorCompilerSpec {

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

  class N extends DataModel[N] {

    val n = new IntOption()

    override def reset(): Unit = {
      n.setNull()
    }
    override def copyFrom(other: N): Unit = {
      n.copyFrom(other.n)
    }

    def getIOption: IntOption = n
  }

  class CoGroupOperator {

    private[this] val n = new N

    @CoGroup
    def cogroup(
      hogeList: JList[Hoge], fooList: JList[Foo],
      hogeResult: Result[Hoge], fooResult: Result[Foo],
      hogeError: Result[Hoge], fooError: Result[Foo],
      nResult: Result[N], n: Int): Unit = {
      if (hogeList.size == 1 && fooList.size == 1) {
        hogeResult.add(hogeList(0))
        fooResult.add(fooList(0))
      } else {
        hogeList.foreach(hogeError.add)
        fooList.foreach(fooError.add)
      }
      this.n.n.modify(n)
      nResult.add(this.n)
    }

    @CoGroup
    def cogroupp[H <: HogeP, F <: FooP](
      hogeList: JList[H], fooList: JList[F],
      hogeResult: Result[H], fooResult: Result[F],
      hogeError: Result[H], fooError: Result[F],
      nResult: Result[N], n: Int): Unit = {
      if (hogeList.size == 1 && fooList.size == 1) {
        hogeResult.add(hogeList(0))
        fooResult.add(fooList(0))
      } else {
        hogeList.foreach(hogeError.add)
        fooList.foreach(fooError.add)
      }
      this.n.n.modify(n)
      nResult.add(this.n)
    }
  }
}
