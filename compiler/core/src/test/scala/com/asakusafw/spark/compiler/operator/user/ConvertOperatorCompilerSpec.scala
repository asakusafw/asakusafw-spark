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
import com.asakusafw.vocabulary.operator.Convert

@RunWith(classOf[JUnitRunner])
class ConvertOperatorCompilerSpecTest extends ConvertOperatorCompilerSpec

class ConvertOperatorCompilerSpec extends FlatSpec with LoadClassSugar {

  import ConvertOperatorCompilerSpec._

  behavior of classOf[ConvertOperatorCompiler].getSimpleName

  it should "compile Convert operator" in {
    val operator = OperatorExtractor
      .extract(classOf[Convert], classOf[ConvertOperator], "convert")
      .input("input", ClassDescription.of(classOf[InputModel]))
      .output("original", ClassDescription.of(classOf[InputModel]))
      .output("out", ClassDescription.of(classOf[OutputModel]))
      .argument("n", ImmediateDescription.of(10))
      .build()

    val classpath = Files.createTempDirectory("ConvertOperatorCompilerSpec").toFile
    implicit val context = OperatorCompiler.Context(
      flowId = "flowId",
      jpContext = new MockJobflowProcessorContext(
        new CompilerOptions("buildid", "", Map.empty[String, String]),
        Thread.currentThread.getContextClassLoader,
        classpath))

    val thisType = OperatorCompiler.compile(operator, OperatorType.MapType)
    val cls = loadClass(thisType.getClassName, classpath).asSubclass(classOf[Fragment[InputModel]])

    val out1 = {
      val builder = new OutputFragmentClassBuilder(
        context.flowId, classOf[InputModel].asType)
      val cls = loadClass(builder.thisType.getClassName, builder.build()).asSubclass(classOf[OutputFragment[InputModel]])
      cls.newInstance()
    }

    val out2 = {
      val builder = new OutputFragmentClassBuilder(context.flowId, classOf[OutputModel].asType)
      val cls = loadClass(builder.thisType.getClassName, builder.build()).asSubclass(classOf[OutputFragment[OutputModel]])
      cls.newInstance()
    }

    val fragment = cls.getConstructor(classOf[Fragment[_]], classOf[Fragment[_]]).newInstance(out1, out2)

    val dm = new InputModel()
    for (i <- 0 until 10) {
      dm.i.modify(i)
      dm.l.modify(i)
      fragment.add(dm)
    }
    assert(out1.buffer.size === 10)
    assert(out2.buffer.size === 10)
    out1.buffer.zipWithIndex.foreach {
      case (dm, i) =>
        assert(dm.i.get === i)
        assert(dm.l.get === i)
    }
    out2.buffer.zipWithIndex.foreach {
      case (dm, i) =>
        assert(dm.l.get === 10 * i)
    }
    fragment.reset()
    assert(out1.buffer.size === 0)
    assert(out2.buffer.size === 0)
  }
}

object ConvertOperatorCompilerSpec {

  class InputModel extends DataModel[InputModel] {

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

  class OutputModel extends DataModel[OutputModel] {

    val l: LongOption = new LongOption()

    override def reset: Unit = {
      l.setNull()
    }

    override def copyFrom(other: OutputModel): Unit = {
      l.copyFrom(other.l)
    }

    def getLOption: LongOption = l
  }

  class ConvertOperator {

    private[this] val out = new OutputModel()

    @Convert
    def convert(in: InputModel, n: Int): OutputModel = {
      out.reset()
      out.l.modify(n * in.l.get)
      out
    }
  }
}
