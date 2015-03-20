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
import com.asakusafw.runtime.core.Result
import com.asakusafw.runtime.model.DataModel
import com.asakusafw.runtime.value._
import com.asakusafw.spark.compiler.spi.UserOperatorCompiler
import com.asakusafw.spark.runtime.fragment._
import com.asakusafw.spark.tools.asm._
import com.asakusafw.vocabulary.operator.Update
import com.asakusafw.lang.compiler.model.graph.UserOperator
import com.asakusafw.lang.compiler.model.testing.OperatorExtractor

@RunWith(classOf[JUnitRunner])
class UpdateOperatorCompilerSpecTest extends UpdateOperatorCompilerSpec

class UpdateOperatorCompilerSpec extends FlatSpec with LoadClassSugar {
  import UpdateOperatorCompilerSpec._

  behavior of classOf[UpdateOperatorCompiler].getSimpleName

  def resolvers = UserOperatorCompiler(Thread.currentThread.getContextClassLoader)

  it should "compile Update operator" in {
    val operator = OperatorExtractor.extract( //
      classOf[Update], classOf[UpdateOperator], "update")
      .input("in", ClassDescription.of(classOf[InputOutputModel]))
      .output("out", ClassDescription.of(classOf[InputOutputModel]))
      .argument("rate", ImmediateDescription.of(100))
      .build();

    val compiler = resolvers(classOf[Update])
    val classpath = Files.createTempDirectory("UpdateOperatorCompilerSpec").toFile
    val context = OperatorCompiler.Context(
      flowId = "flowId",
      jpContext = new MockJobflowProcessorContext(
        new CompilerOptions("buildid", "", Map.empty[String, String]),
        Thread.currentThread.getContextClassLoader,
        classpath))
    val thisType = compiler.compile(operator)(context)
    val cls = loadClass(thisType.getClassName, classpath) //
      .asSubclass(classOf[Fragment[InputOutputModel]])

    val out = {
      val builder = new OutputFragmentClassBuilder( //
        context.flowId, classOf[InputOutputModel].asType)
      val cls = loadClass(builder.thisType.getClassName, builder.build()). //
        asSubclass(classOf[OutputFragment[InputOutputModel]])
      cls.newInstance
    }

    val fragment = cls.getConstructor(classOf[Fragment[_]]).newInstance(out)

    val dm = new InputOutputModel()
    for (i <- 0 until 10) {
      dm.reset()
      if (i % 5 != 0) {
        dm.i.modify(i)
      }
      if (i % 3 != 0) {
        dm.l.modify(i * 10L)
      }
      fragment.add(dm)
    }
    assert(out.buffer.size === 10)
    out.buffer.zipWithIndex.foreach {
      case (dm, i) =>
        assert(dm.i.get === i)
    }
    fragment.reset()
    assert(out.buffer.size === 0)
  }
}

object UpdateOperatorCompilerSpec {

  class InputOutputModel extends DataModel[InputOutputModel] {

    val i: IntOption = new IntOption()
    val l: LongOption = new LongOption()

    override def reset: Unit = {
      i.setNull()
      l.setNull()
    }

    override def copyFrom(other: InputOutputModel): Unit = {
      i.copyFrom(other.i)
      l.copyFrom(other.l)
    }

    def getIOption: IntOption = i
    def getLOption: LongOption = l

    def setIOption(v: IntOption): IntOption = { i.copyFrom(v); i }
    def setLOption(v: LongOption): LongOption = { l.copyFrom(v); l }
  }

  class UpdateOperator {

    @Update
    def update(item: InputOutputModel, rate: Int): Unit = {
      val current = item.getIOption
      if (current.isNull) {
        item.setIOption(null)
      } else {
        item.setIOption(new IntOption(rate * current.get))
      }
    }
  }
}
