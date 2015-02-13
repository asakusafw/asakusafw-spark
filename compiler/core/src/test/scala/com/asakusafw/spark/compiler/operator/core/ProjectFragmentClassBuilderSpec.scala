package com.asakusafw.spark.compiler.operator
package core

import org.junit.runner.RunWith
import org.scalatest.FlatSpec
import org.scalatest.junit.JUnitRunner

import java.nio.file.Files

import com.asakusafw.lang.compiler.api.CompilerOptions
import com.asakusafw.lang.compiler.api.mock.MockJobflowProcessorContext
import com.asakusafw.lang.compiler.api.reference.DataModelReference
import com.asakusafw.lang.compiler.model.description.ClassDescription
import com.asakusafw.lang.compiler.model.graph.CoreOperator
import com.asakusafw.lang.compiler.model.graph.CoreOperator.CoreOperatorKind
import com.asakusafw.runtime.model.DataModel
import com.asakusafw.runtime.value._
import com.asakusafw.spark.runtime.fragment._
import com.asakusafw.spark.tools.asm._
import com.asakusafw.spark.compiler.spi.CoreOperatorCompiler

@RunWith(classOf[JUnitRunner])
class ProjectFragmentClassBuilderSpecTest extends ProjectFragmentClassBuilderSpec

class ProjectFragmentClassBuilderSpec extends FlatSpec with LoadClassSugar {

  import ProjectFragmentClassBuilderSpec._

  behavior of classOf[ProjectOperatorCompiler].getSimpleName

  val resolvers = CoreOperatorCompiler(Thread.currentThread.getContextClassLoader)

  it should "compile Project operator" in {
    val operator = CoreOperator.builder(CoreOperatorKind.PROJECT)
      .input("input", ClassDescription.of(classOf[InputModel]))
      .output("output", ClassDescription.of(classOf[OutputModel]))
      .build()

    val compiler = resolvers(CoreOperatorKind.PROJECT)
    val builder = compiler.compile(operator)(
      compiler.Context(
        jpContext = new MockJobflowProcessorContext(
          new CompilerOptions(""),
          Thread.currentThread.getContextClassLoader,
          Files.createTempDirectory("ProjectFragmentClassBuilderSpec").toFile)))
    val cls = loadClass(builder.thisType.getClassName, builder.build())
      .asSubclass(classOf[Fragment[InputModel]])

    val out = {
      val builder = new OutputFragmentClassBuilder(classOf[OutputModel].asType)
      val cls = loadClass(builder.thisType.getClassName, builder.build()).asSubclass(classOf[OutputFragment[OutputModel]])
      cls.newInstance
    }

    val fragment = cls.getConstructor(classOf[Fragment[_]]).newInstance(out)

    val dm = new InputModel()
    for (i <- 0 until 10) {
      dm.i.modify(i)
      dm.l.modify(i)
      fragment.add(dm)
    }
    assert(out.buffer.size == 10)
    out.buffer.zipWithIndex.foreach {
      case (dm, i) =>
        assert(dm.i.get === i)
    }
    fragment.reset()
    assert(out.buffer.size === 0)
  }
}

object ProjectFragmentClassBuilderSpec {

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

    val i: IntOption = new IntOption()

    override def reset: Unit = {
      i.setNull()
    }

    override def copyFrom(other: OutputModel): Unit = {
      i.copyFrom(other.i)
    }

    def getIOption: IntOption = i
  }
}
