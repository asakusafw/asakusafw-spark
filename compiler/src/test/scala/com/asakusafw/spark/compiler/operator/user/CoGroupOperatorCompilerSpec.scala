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
import com.asakusafw.vocabulary.operator.CoGroup

@RunWith(classOf[JUnitRunner])
class CoGroupOperatorCompilerSpecTest extends CoGroupOperatorCompilerSpec

class CoGroupOperatorCompilerSpec extends FlatSpec with LoadClassSugar with TempDir with UsingCompilerContext {

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
    implicit val context = newContext("flowId", classpath)

    val thisType = OperatorCompiler.compile(operator, OperatorType.CoGroupType)(
      context.subplanCompilerContext.operatorCompilerContext)
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
      fragment.reset()
      val hoges = Seq.empty[Hoge]
      val foos = Seq.empty[Foo]
      fragment.add(Seq(hoges.iterator, foos.iterator))
      assert(hogeResult.iterator.size === 0)
      assert(fooResult.iterator.size === 0)
      assert(hogeError.iterator.size === 0)
      assert(fooError.iterator.size === 0)
      val nResults = nResult.iterator.toSeq
      assert(nResults.size === 1)
      assert(nResults.head.n.get === 10)
    }

    fragment.reset()
    assert(hogeResult.iterator.size === 0)
    assert(fooResult.iterator.size === 0)
    assert(hogeError.iterator.size === 0)
    assert(fooError.iterator.size === 0)
    assert(nResult.iterator.size === 0)

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
      val hogeResults = hogeResult.iterator.toSeq
      assert(hogeResults.size === 1)
      assert(hogeResults.head.id.get === hoge.id.get)
      val fooResults = fooResult.iterator.toSeq
      assert(fooResults.size === 1)
      assert(fooResults.head.id.get === foo.id.get)
      assert(fooResults.head.hogeId.get === foo.hogeId.get)
      val hogeErrors = hogeError.iterator.toSeq
      assert(hogeErrors.size === 0)
      val fooErrors = fooError.iterator.toSeq
      assert(fooErrors.size === 0)
      val nResults = nResult.iterator.toSeq
      assert(nResults.size === 1)
      assert(nResults.head.n.get === 10)
    }

    fragment.reset()
    assert(hogeResult.iterator.size === 0)
    assert(fooResult.iterator.size === 0)
    assert(hogeError.iterator.size === 0)
    assert(fooError.iterator.size === 0)
    assert(nResult.iterator.size === 0)

    {
      fragment.reset()
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
      val hogeResults = hogeResult.iterator.toSeq
      assert(hogeResults.size === 0)
      val fooResults = fooResult.iterator.toSeq
      assert(fooResults.size === 0)
      val hogeErrors = hogeError.iterator.toSeq
      assert(hogeErrors.size === 1)
      assert(hogeErrors.head.id.get === hoge.id.get)
      val fooErrors = fooError.iterator.toSeq
      assert(fooErrors.size === 10)
      fooErrors.zip(foos).foreach {
        case (actual, expected) =>
          assert(actual.id.get === expected.id.get)
          assert(actual.hogeId.get === expected.hogeId.get)
      }
      val nResults = nResult.iterator.toSeq
      assert(nResults.size === 1)
      assert(nResults.head.n.get === 10)
    }

    fragment.reset()
    assert(hogeResult.iterator.size === 0)
    assert(fooResult.iterator.size === 0)
    assert(hogeError.iterator.size === 0)
    assert(fooError.iterator.size === 0)
    assert(nResult.iterator.size === 0)
  }
}

object CoGroupOperatorCompilerSpec {

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

  class N extends DataModel[N] with Writable {

    val n = new IntOption()

    override def reset(): Unit = {
      n.setNull()
    }
    override def copyFrom(other: N): Unit = {
      n.copyFrom(other.n)
    }
    override def readFields(in: DataInput): Unit = {
      n.readFields(in)
    }
    override def write(out: DataOutput): Unit = {
      n.write(out)
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
