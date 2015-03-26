package com.asakusafw.spark.compiler.operator
package user

import org.junit.runner.RunWith
import org.scalatest.FlatSpec
import org.scalatest.junit.JUnitRunner

import java.nio.file.Files

import scala.collection.JavaConversions._

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
import com.asakusafw.vocabulary.operator.Branch

@RunWith(classOf[JUnitRunner])
class BranchOperatorCompilerSpecTest extends BranchOperatorCompilerSpec

class BranchOperatorCompilerSpec extends FlatSpec with LoadClassSugar {

  import BranchOperatorCompilerSpec._

  behavior of classOf[BranchOperatorCompiler].getSimpleName

  it should "compile Branch operator" in {
    val operator = OperatorExtractor
      .extract(classOf[Branch], classOf[BranchOperator], "branch")
      .input("input", ClassDescription.of(classOf[InputModel]))
      .output("low", ClassDescription.of(classOf[InputModel]))
      .output("high", ClassDescription.of(classOf[InputModel]))
      .argument("n", ImmediateDescription.of(10))
      .build()

    val classpath = Files.createTempDirectory("BranchOperatorCompilerSpec").toFile
    implicit val context = OperatorCompiler.Context(
      flowId = "flowId",
      jpContext = new MockJobflowProcessorContext(
        new CompilerOptions("buildid", "", Map.empty[String, String]),
        Thread.currentThread.getContextClassLoader,
        classpath))

    val thisType = OperatorCompiler.compile(operator, OperatorType.MapType)
    val cls = loadClass(thisType.getClassName, classpath).asSubclass(classOf[Fragment[InputModel]])

    val (out1, out2) = {
      val builder = new OutputFragmentClassBuilder(
        context.flowId, classOf[InputModel].asType)
      val cls = loadClass(builder.thisType.getClassName, builder.build()).asSubclass(classOf[OutputFragment[InputModel]])
      (cls.newInstance(), cls.newInstance())
    }

    val fragment = cls.getConstructor(classOf[Fragment[_]], classOf[Fragment[_]]).newInstance(out1, out2)

    val dm = new InputModel()
    for (i <- 0 until 10) {
      dm.i.modify(i)
      fragment.add(dm)
    }
    assert(out1.buffer.size === 5)
    assert(out2.buffer.size === 5)
    out1.buffer.zipWithIndex.foreach {
      case (dm, i) =>
        assert(dm.i.get === i)
    }
    out2.buffer.zipWithIndex.foreach {
      case (dm, i) =>
        assert(dm.i.get === i + 5)
    }
    fragment.reset()
    assert(out1.buffer.size === 0)
  }

  it should "compile Branch operator with projective model" in {
    val operator = OperatorExtractor
      .extract(classOf[Branch], classOf[BranchOperator], "branchp")
      .input("input", ClassDescription.of(classOf[InputModel]))
      .output("low", ClassDescription.of(classOf[InputModel]))
      .output("high", ClassDescription.of(classOf[InputModel]))
      .argument("n", ImmediateDescription.of(10))
      .build()

    val classpath = Files.createTempDirectory("BranchOperatorCompilerSpec").toFile
    implicit val context = OperatorCompiler.Context(
      flowId = "flowId",
      jpContext = new MockJobflowProcessorContext(
        new CompilerOptions("buildid", "", Map.empty[String, String]),
        Thread.currentThread.getContextClassLoader,
        classpath))

    val thisType = OperatorCompiler.compile(operator, OperatorType.MapType)
    val cls = loadClass(thisType.getClassName, classpath).asSubclass(classOf[Fragment[InputModel]])

    val (out1, out2) = {
      val builder = new OutputFragmentClassBuilder(
        context.flowId, classOf[InputModel].asType)
      val cls = loadClass(builder.thisType.getClassName, builder.build()).asSubclass(classOf[OutputFragment[InputModel]])
      (cls.newInstance(), cls.newInstance())
    }

    val fragment = cls.getConstructor(classOf[Fragment[_]], classOf[Fragment[_]]).newInstance(out1, out2)

    val dm = new InputModel()
    for (i <- 0 until 10) {
      dm.i.modify(i)
      fragment.add(dm)
    }
    assert(out1.buffer.size === 5)
    assert(out2.buffer.size === 5)
    out1.buffer.zipWithIndex.foreach {
      case (dm, i) =>
        assert(dm.i.get === i)
    }
    out2.buffer.zipWithIndex.foreach {
      case (dm, i) =>
        assert(dm.i.get === i + 5)
    }
    fragment.reset()
    assert(out1.buffer.size === 0)
  }
}

object BranchOperatorCompilerSpec {

  trait InputP {
    def getIOption: IntOption
  }

  class InputModel extends DataModel[InputModel] with InputP {

    val i: IntOption = new IntOption()

    override def reset: Unit = {
      i.setNull()
    }

    override def copyFrom(other: InputModel): Unit = {
      i.copyFrom(other.i)
    }

    def getIOption: IntOption = i
  }

  class BranchOperator {

    @Branch
    def branch(in: InputModel, n: Int): BranchOperatorCompilerSpecTestBranch = {
      if (in.i.get < 5) {
        BranchOperatorCompilerSpecTestBranch.LOW
      } else {
        BranchOperatorCompilerSpecTestBranch.HIGH
      }
    }

    @Branch
    def branchp[I <: InputP](in: I, n: Int): BranchOperatorCompilerSpecTestBranch = {
      if (in.getIOption.get < 5) {
        BranchOperatorCompilerSpecTestBranch.LOW
      } else {
        BranchOperatorCompilerSpecTestBranch.HIGH
      }
    }
  }
}
