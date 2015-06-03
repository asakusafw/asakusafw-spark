package com.asakusafw.spark.compiler
package operator
package user

import org.junit.runner.RunWith
import org.scalatest.FlatSpec
import org.scalatest.junit.JUnitRunner

import scala.collection.JavaConversions._
import scala.collection.mutable

import org.apache.spark.broadcast.Broadcast

import com.asakusafw.lang.compiler.api.CompilerOptions
import com.asakusafw.lang.compiler.api.testing.MockJobflowProcessorContext
import com.asakusafw.lang.compiler.model.description._
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
import com.asakusafw.vocabulary.operator.Split

@RunWith(classOf[JUnitRunner])
class SplitOperatorCompilerSpecTest extends SplitOperatorCompilerSpec

class SplitOperatorCompilerSpec extends FlatSpec with LoadClassSugar with TempDir {

  import SplitOperatorCompilerSpec._

  behavior of classOf[SplitOperatorCompiler].getSimpleName

  it should "compile Split operator" in {
    val operator = OperatorExtractor
      .extract(classOf[Split], classOf[SplitOperator], "split")
      .input("input", ClassDescription.of(classOf[Baa]))
      .output("left", ClassDescription.of(classOf[Hoge]))
      .output("right", ClassDescription.of(classOf[Foo]))
      .build()

    val classpath = createTempDirectory("SplitOperatorCompilerSpec").toFile
    implicit val context = OperatorCompiler.Context(
      flowId = "flowId",
      jpContext = new MockJobflowProcessorContext(
        new CompilerOptions("buildid", "", Map.empty[String, String]),
        Thread.currentThread.getContextClassLoader,
        classpath),
      branchKeys = new BranchKeysClassBuilder("flowId"),
      broadcastIds = new BroadcastIdsClassBuilder("flowId"))

    val thisType = OperatorCompiler.compile(operator, OperatorType.MapType)
    val cls = loadClass(thisType.getClassName, classpath).asSubclass(classOf[Fragment[Baa]])

    val hoges = new GenericOutputFragment[Hoge]
    val foos = new GenericOutputFragment[Foo]

    val fragment = cls.getConstructor(
      classOf[Map[BroadcastId, Broadcast[_]]],
      classOf[Fragment[_]], classOf[Fragment[_]]).newInstance(Map.empty, hoges, foos)

    val dm = new Baa()
    for (i <- 0 until 10) {
      dm.id.modify(i)
      dm.hoge.modify(s"hoge ${i}")
      dm.foo.modify(s"foo ${i}")
      fragment.add(dm)
    }
    assert(hoges.size === 10)
    assert(foos.size === 10)
    hoges.zipWithIndex.foreach {
      case (dm, i) =>
        assert(dm.id.get === i)
        assert(dm.hoge.getAsString === s"hoge ${i}")
    }
    foos.zipWithIndex.foreach {
      case (dm, i) =>
        assert(dm.hogeId.get === i)
        assert(dm.foo.getAsString === s"foo ${i}")
    }
    fragment.reset()
    assert(hoges.size === 0)
    assert(foos.size === 0)
  }
}

object SplitOperatorCompilerSpec {

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

  class SplitOperator {

    @Split
    def split(baa: Baa, hoges: Result[Hoge], foos: Result[Foo]): Unit = ???
  }
}
