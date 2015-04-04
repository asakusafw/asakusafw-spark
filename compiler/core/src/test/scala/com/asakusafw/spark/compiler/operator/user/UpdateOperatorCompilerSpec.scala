package com.asakusafw.spark.compiler.operator
package user

import org.junit.runner.RunWith
import org.scalatest.FlatSpec
import org.scalatest.junit.JUnitRunner

import java.nio.file.Files

import scala.collection.JavaConversions._
import scala.collection.mutable

import org.apache.spark.broadcast.Broadcast

import com.asakusafw.lang.compiler.api.CompilerOptions
import com.asakusafw.lang.compiler.api.testing.MockJobflowProcessorContext
import com.asakusafw.lang.compiler.model.description._
import com.asakusafw.lang.compiler.model.testing.OperatorExtractor
import com.asakusafw.runtime.model.DataModel
import com.asakusafw.runtime.value._
import com.asakusafw.spark.compiler.spi.{ OperatorCompiler, OperatorType }
import com.asakusafw.spark.runtime.fragment._
import com.asakusafw.spark.tools.asm._
import com.asakusafw.vocabulary.operator.Update

@RunWith(classOf[JUnitRunner])
class UpdateOperatorCompilerSpecTest extends UpdateOperatorCompilerSpec

class UpdateOperatorCompilerSpec extends FlatSpec with LoadClassSugar {

  import UpdateOperatorCompilerSpec._

  behavior of classOf[UpdateOperatorCompiler].getSimpleName

  it should "compile Update operator" in {
    val operator = OperatorExtractor.extract(
      classOf[Update], classOf[UpdateOperator], "update")
      .input("in", ClassDescription.of(classOf[TestModel]))
      .output("out", ClassDescription.of(classOf[TestModel]))
      .argument("rate", ImmediateDescription.of(100))
      .build();

    val classpath = Files.createTempDirectory("UpdateOperatorCompilerSpec").toFile
    implicit val context = OperatorCompiler.Context(
      flowId = "flowId",
      jpContext = new MockJobflowProcessorContext(
        new CompilerOptions("buildid", "", Map.empty[String, String]),
        Thread.currentThread.getContextClassLoader,
        classpath),
      shuffleKeyTypes = mutable.Set.empty)

    val thisType = OperatorCompiler.compile(operator, OperatorType.MapType)
    val cls = loadClass(thisType.getClassName, classpath)
      .asSubclass(classOf[Fragment[TestModel]])

    val out = {
      val builder = new OutputFragmentClassBuilder(
        context.flowId, classOf[TestModel].asType)
      val cls = loadClass(builder.thisType.getClassName, builder.build())
        .asSubclass(classOf[OutputFragment[TestModel]])
      cls.newInstance()
    }

    val fragment = cls
      .getConstructor(classOf[Map[Long, Broadcast[_]]], classOf[Fragment[_]])
      .newInstance(Map.empty, out)

    val dm = new TestModel()
    for (i <- 0 until 10) {
      dm.reset()
      dm.i.modify(i)
      fragment.add(dm)
    }
    assert(out.buffer.size === 10)
    out.buffer.zipWithIndex.foreach {
      case (dm, i) =>
        assert(dm.i.get === i)
        assert(dm.l.get === i * 100)
    }
    fragment.reset()
    assert(out.buffer.size === 0)
  }

  it should "compile Update operator with projective model" in {
    val operator = OperatorExtractor.extract(
      classOf[Update], classOf[UpdateOperator], "updatep")
      .input("in", ClassDescription.of(classOf[TestModel]))
      .output("out", ClassDescription.of(classOf[TestModel]))
      .argument("rate", ImmediateDescription.of(100))
      .build();

    val classpath = Files.createTempDirectory("UpdateOperatorCompilerSpec").toFile
    implicit val context = OperatorCompiler.Context(
      flowId = "flowId",
      jpContext = new MockJobflowProcessorContext(
        new CompilerOptions("buildid", "", Map.empty[String, String]),
        Thread.currentThread.getContextClassLoader,
        classpath),
      shuffleKeyTypes = mutable.Set.empty)

    val thisType = OperatorCompiler.compile(operator, OperatorType.MapType)
    val cls = loadClass(thisType.getClassName, classpath)
      .asSubclass(classOf[Fragment[TestModel]])

    val out = {
      val builder = new OutputFragmentClassBuilder(
        context.flowId, classOf[TestModel].asType)
      val cls = loadClass(builder.thisType.getClassName, builder.build())
        .asSubclass(classOf[OutputFragment[TestModel]])
      cls.newInstance
    }

    val fragment = cls
      .getConstructor(classOf[Map[Long, Broadcast[_]]], classOf[Fragment[_]])
      .newInstance(Map.empty, out)

    val dm = new TestModel()
    for (i <- 0 until 10) {
      dm.reset()
      dm.i.modify(i)
      fragment.add(dm)
    }
    assert(out.buffer.size === 10)
    out.buffer.zipWithIndex.foreach {
      case (dm, i) =>
        assert(dm.i.get === i)
        assert(dm.l.get === i * 100)
    }
    fragment.reset()
    assert(out.buffer.size === 0)
  }
}

object UpdateOperatorCompilerSpec {

  trait TestP {
    def getIOption: IntOption
    def getLOption: LongOption
  }

  class TestModel extends DataModel[TestModel] with TestP {

    val i: IntOption = new IntOption()
    val l: LongOption = new LongOption()

    override def reset: Unit = {
      i.setNull()
      l.setNull()
    }

    override def copyFrom(other: TestModel): Unit = {
      i.copyFrom(other.i)
      l.copyFrom(other.l)
    }

    def getIOption: IntOption = i
    def getLOption: LongOption = l
  }

  class UpdateOperator {

    @Update
    def update(item: TestModel, rate: Int): Unit = {
      item.l.modify(rate * item.i.get)
    }

    @Update
    def updatep[T <: TestP](item: T, rate: Int): Unit = {
      item.getLOption.modify(rate * item.getIOption.get)
    }
  }
}
