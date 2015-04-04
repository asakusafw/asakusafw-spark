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
import com.asakusafw.runtime.core.Result
import com.asakusafw.runtime.model.DataModel
import com.asakusafw.runtime.value._
import com.asakusafw.spark.compiler.spi.{ OperatorCompiler, OperatorType }
import com.asakusafw.spark.runtime.fragment._
import com.asakusafw.spark.tools.asm._
import com.asakusafw.vocabulary.operator.Extract

@RunWith(classOf[JUnitRunner])
class ExtractOperatorCompilerSpecTest extends ExtractOperatorCompilerSpec

class ExtractOperatorCompilerSpec extends FlatSpec with LoadClassSugar {

  import ExtractOperatorCompilerSpec._

  behavior of classOf[ExtractOperatorCompiler].getSimpleName

  it should "compile Extract operator" in {
    val operator = OperatorExtractor
      .extract(classOf[Extract], classOf[ExtractOperator], "extract")
      .input("input", ClassDescription.of(classOf[InputModel]))
      .output("output1", ClassDescription.of(classOf[IntOutputModel]))
      .output("output2", ClassDescription.of(classOf[LongOutputModel]))
      .argument("n", ImmediateDescription.of(10))
      .build()

    val classpath = Files.createTempDirectory("ExtractOperatorCompilerSpec").toFile
    implicit val context = OperatorCompiler.Context(
      flowId = "flowId",
      jpContext = new MockJobflowProcessorContext(
        new CompilerOptions("buildid", "", Map.empty[String, String]),
        Thread.currentThread.getContextClassLoader,
        classpath),
      shuffleKeyTypes = mutable.Set.empty)

    val thisType = OperatorCompiler.compile(operator, OperatorType.MapType)
    val cls = loadClass(thisType.getClassName, classpath).asSubclass(classOf[Fragment[InputModel]])

    val out1 = {
      val builder = new OutputFragmentClassBuilder(
        context.flowId, classOf[IntOutputModel].asType)
      val cls = loadClass(builder.thisType.getClassName, builder.build()).asSubclass(classOf[OutputFragment[IntOutputModel]])
      cls.newInstance()
    }

    val out2 = {
      val builder = new OutputFragmentClassBuilder(context.flowId, classOf[LongOutputModel].asType)
      val cls = loadClass(builder.thisType.getClassName, builder.build()).asSubclass(classOf[OutputFragment[LongOutputModel]])
      cls.newInstance()
    }

    val fragment = cls
      .getConstructor(
        classOf[Map[Long, Broadcast[_]]],
        classOf[Fragment[_]], classOf[Fragment[_]])
      .newInstance(Map.empty, out1, out2)

    val dm = new InputModel()
    for (i <- 0 until 10) {
      dm.i.modify(i)
      dm.l.modify(i)
      fragment.add(dm)
    }
    assert(out1.buffer.size === 10)
    assert(out2.buffer.size === 100)
    out1.buffer.zipWithIndex.foreach {
      case (dm, i) =>
        assert(dm.i.get === i)
    }
    out2.buffer.zipWithIndex.foreach {
      case (dm, i) =>
        assert(dm.l.get === i / 10)
    }
    fragment.reset()
    assert(out1.buffer.size === 0)
  }

  it should "compile Extract operator with projective model" in {
    val operator = OperatorExtractor
      .extract(classOf[Extract], classOf[ExtractOperator], "extractp")
      .input("input", ClassDescription.of(classOf[InputModel]))
      .output("output1", ClassDescription.of(classOf[IntOutputModel]))
      .output("output2", ClassDescription.of(classOf[LongOutputModel]))
      .argument("n", ImmediateDescription.of(10))
      .build()

    val classpath = Files.createTempDirectory("ExtractOperatorCompilerSpec").toFile
    implicit val context = OperatorCompiler.Context(
      flowId = "flowId",
      jpContext = new MockJobflowProcessorContext(
        new CompilerOptions("buildid", "", Map.empty[String, String]),
        Thread.currentThread.getContextClassLoader,
        classpath),
      shuffleKeyTypes = mutable.Set.empty)

    val thisType = OperatorCompiler.compile(operator, OperatorType.MapType)
    val cls = loadClass(thisType.getClassName, classpath).asSubclass(classOf[Fragment[InputModel]])

    val out1 = {
      val builder = new OutputFragmentClassBuilder(
        context.flowId, classOf[IntOutputModel].asType)
      val cls = loadClass(builder.thisType.getClassName, builder.build()).asSubclass(classOf[OutputFragment[IntOutputModel]])
      cls.newInstance()
    }

    val out2 = {
      val builder = new OutputFragmentClassBuilder(context.flowId, classOf[LongOutputModel].asType)
      val cls = loadClass(builder.thisType.getClassName, builder.build()).asSubclass(classOf[OutputFragment[LongOutputModel]])
      cls.newInstance()
    }

    val fragment = cls
      .getConstructor(
        classOf[Map[Long, Broadcast[_]]],
        classOf[Fragment[_]], classOf[Fragment[_]])
      .newInstance(Map.empty, out1, out2)

    val dm = new InputModel()
    for (i <- 0 until 10) {
      dm.i.modify(i)
      dm.l.modify(i)
      fragment.add(dm)
    }
    assert(out1.buffer.size === 10)
    assert(out2.buffer.size === 100)
    out1.buffer.zipWithIndex.foreach {
      case (dm, i) =>
        assert(dm.i.get === i)
    }
    out2.buffer.zipWithIndex.foreach {
      case (dm, i) =>
        assert(dm.l.get === i / 10)
    }
    fragment.reset()
    assert(out1.buffer.size === 0)
  }
}

object ExtractOperatorCompilerSpec {

  trait InputP {
    def getIOption: IntOption
    def getLOption: LongOption
  }

  class InputModel extends DataModel[InputModel] with InputP {

    val i: IntOption = new IntOption()
    val l: LongOption = new LongOption()

    override def reset: Unit = {
      i.setNull()
      l.setNull()
    }

    override def copyFrom(other: InputModel): Unit = {
      i.copyFrom(other.i)
      l.copyFrom(other.l)
    }

    def getIOption: IntOption = i
    def getLOption: LongOption = l
  }

  trait IntOutputP {
    def getIOption: IntOption
  }

  class IntOutputModel extends DataModel[IntOutputModel] with IntOutputP {

    val i: IntOption = new IntOption()

    override def reset: Unit = {
      i.setNull()
    }

    override def copyFrom(other: IntOutputModel): Unit = {
      i.copyFrom(other.i)
    }

    def getIOption: IntOption = i
  }

  trait LongOutputP {
    def getLOption: LongOption
  }

  class LongOutputModel extends DataModel[LongOutputModel] with LongOutputP {

    val l: LongOption = new LongOption()

    override def reset: Unit = {
      l.setNull()
    }

    override def copyFrom(other: LongOutputModel): Unit = {
      l.copyFrom(other.l)
    }

    def getLOption: LongOption = l
  }

  class ExtractOperator {

    private[this] val i = new IntOutputModel()
    private[this] val l = new LongOutputModel()

    @Extract
    def extract(in: InputModel, out1: Result[IntOutputModel], out2: Result[LongOutputModel], n: Int): Unit = {
      i.getIOption.copyFrom(in.getIOption)
      out1.add(i)
      for (_ <- 0 until n) {
        l.getLOption.copyFrom(in.getLOption)
        out2.add(l)
      }
    }

    @Extract
    def extractp[I <: InputP, IO <: IntOutputP, LO <: LongOutputP](in: I, out1: Result[IO], out2: Result[LO], n: Int): Unit = {
      i.getIOption.copyFrom(in.getIOption)
      out1.add(i.asInstanceOf[IO])
      for (_ <- 0 until n) {
        l.getLOption.copyFrom(in.getLOption)
        out2.add(l.asInstanceOf[LO])
      }
    }
  }
}
