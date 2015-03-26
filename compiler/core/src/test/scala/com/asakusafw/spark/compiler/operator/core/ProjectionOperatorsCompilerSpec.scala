package com.asakusafw.spark.compiler.operator
package core

import org.junit.runner.RunWith
import org.scalatest.FlatSpec
import org.scalatest.junit.JUnitRunner

import java.nio.file.Files

import scala.collection.JavaConversions._

import com.asakusafw.lang.compiler.api.CompilerOptions
import com.asakusafw.lang.compiler.api.testing.MockJobflowProcessorContext
import com.asakusafw.lang.compiler.api.reference.DataModelReference
import com.asakusafw.lang.compiler.model.description.ClassDescription
import com.asakusafw.lang.compiler.model.graph.CoreOperator
import com.asakusafw.lang.compiler.model.graph.CoreOperator.CoreOperatorKind
import com.asakusafw.runtime.model.DataModel
import com.asakusafw.runtime.value._
import com.asakusafw.spark.compiler.spi.{ OperatorCompiler, OperatorType }
import com.asakusafw.spark.runtime.fragment._
import com.asakusafw.spark.tools.asm._

@RunWith(classOf[JUnitRunner])
class ProjectionOperatorsCompilerSpecTest extends ProjectionOperatorsCompilerSpec

class ProjectionOperatorsCompilerSpec extends FlatSpec with LoadClassSugar {

  import ProjectionOperatorsCompilerSpec._

  behavior of classOf[ProjectionOperatorsCompiler].getSimpleName

  it should "compile Project operator" in {
    val operator = CoreOperator.builder(CoreOperatorKind.PROJECT)
      .input("input", ClassDescription.of(classOf[ProjectInputModel]))
      .output("output", ClassDescription.of(classOf[ProjectOutputModel]))
      .build()

    val classpath = Files.createTempDirectory("ProjectionOperatorsCompilerSpec").toFile
    implicit val context = OperatorCompiler.Context(
      flowId = "flowId",
      jpContext = new MockJobflowProcessorContext(
        new CompilerOptions("buildid", "", Map.empty[String, String]),
        Thread.currentThread.getContextClassLoader,
        classpath))

    val thisType = OperatorCompiler.compile(operator, OperatorType.MapType)
    val cls = loadClass(thisType.getClassName, classpath).asSubclass(classOf[Fragment[ProjectInputModel]])

    val out = {
      val builder = new OutputFragmentClassBuilder(context.flowId, classOf[ProjectOutputModel].asType)
      val cls = loadClass(builder.thisType.getClassName, builder.build())
        .asSubclass(classOf[OutputFragment[ProjectOutputModel]])
      cls.newInstance()
    }

    val fragment = cls.getConstructor(classOf[Fragment[_]]).newInstance(out)

    val dm = new ProjectInputModel()
    for (i <- 0 until 10) {
      dm.i.modify(i)
      dm.l.modify(i)
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

  it should "compile Extend operator" in {
    val operator = CoreOperator.builder(CoreOperatorKind.EXTEND)
      .input("input", ClassDescription.of(classOf[ExtendInputModel]))
      .output("output", ClassDescription.of(classOf[ExtendOutputModel]))
      .build()

    val classpath = Files.createTempDirectory("ExtendOperatorCompilerSpec").toFile
    implicit val context = OperatorCompiler.Context(
      flowId = "flowId",
      jpContext = new MockJobflowProcessorContext(
        new CompilerOptions("buildid", "", Map.empty[String, String]),
        Thread.currentThread.getContextClassLoader,
        classpath))

    val thisType = OperatorCompiler.compile(operator, OperatorType.MapType)
    val cls = loadClass(thisType.getClassName, classpath).asSubclass(classOf[Fragment[ExtendInputModel]])

    val out = {
      val builder = new OutputFragmentClassBuilder(context.flowId, classOf[ExtendOutputModel].asType)
      val cls = loadClass(builder.thisType.getClassName, builder.build())
        .asSubclass(classOf[OutputFragment[ExtendOutputModel]])
      cls.newInstance()
    }

    val fragment = cls.getConstructor(classOf[Fragment[_]]).newInstance(out)

    val dm = new ExtendInputModel()
    for (i <- 0 until 10) {
      dm.i.modify(i)
      fragment.add(dm)
    }
    assert(out.buffer.size === 10)
    out.buffer.zipWithIndex.foreach {
      case (dm, i) =>
        assert(dm.i.get === i)
        assert(dm.l.isNull)
    }
    fragment.reset()
    assert(out.buffer.size === 0)
  }

  it should "compile Restructure operator" in {
    val operator = CoreOperator.builder(CoreOperatorKind.RESTRUCTURE)
      .input("input", ClassDescription.of(classOf[RestructureInputModel]))
      .output("output", ClassDescription.of(classOf[RestructureOutputModel]))
      .build()

    val classpath = Files.createTempDirectory("RestructureOperatorCompilerSpec").toFile
    implicit val context = OperatorCompiler.Context(
      flowId = "flowId",
      jpContext = new MockJobflowProcessorContext(
        new CompilerOptions("buildid", "", Map.empty[String, String]),
        Thread.currentThread.getContextClassLoader,
        classpath))

    val thisType = OperatorCompiler.compile(operator, OperatorType.MapType)
    val cls = loadClass(thisType.getClassName, classpath).asSubclass(classOf[Fragment[RestructureInputModel]])

    val out = {
      val builder = new OutputFragmentClassBuilder(context.flowId, classOf[RestructureOutputModel].asType)
      val cls = loadClass(builder.thisType.getClassName, builder.build())
        .asSubclass(classOf[OutputFragment[RestructureOutputModel]])
      cls.newInstance()
    }

    val fragment = cls.getConstructor(classOf[Fragment[_]]).newInstance(out)

    val dm = new RestructureInputModel()
    for (i <- 0 until 10) {
      dm.i.modify(i)
      fragment.add(dm)
    }
    assert(out.buffer.size === 10)
    out.buffer.zipWithIndex.foreach {
      case (dm, i) =>
        assert(dm.i.get === i)
        assert(dm.d.isNull)
    }
    fragment.reset()
    assert(out.buffer.size === 0)
  }
}

object ProjectionOperatorsCompilerSpec {

  class ProjectInputModel extends DataModel[ProjectInputModel] {

    val i: IntOption = new IntOption()
    val l: LongOption = new LongOption()

    override def reset: Unit = {
      i.setNull()
      l.setNull()
    }

    override def copyFrom(other: ProjectInputModel): Unit = {
      i.copyFrom(other.i)
      l.copyFrom(other.l)
    }

    def getIOption: IntOption = i
    def getLOption: LongOption = l
  }

  class ProjectOutputModel extends DataModel[ProjectOutputModel] {

    val i: IntOption = new IntOption()

    override def reset: Unit = {
      i.setNull()
    }

    override def copyFrom(other: ProjectOutputModel): Unit = {
      i.copyFrom(other.i)
    }

    def getIOption: IntOption = i
  }

  class ExtendInputModel extends DataModel[ExtendInputModel] {

    val i: IntOption = new IntOption()

    override def reset: Unit = {
      i.setNull()
    }

    override def copyFrom(other: ExtendInputModel): Unit = {
      i.copyFrom(other.i)
    }

    def getIOption: IntOption = i
  }

  class ExtendOutputModel extends DataModel[ExtendOutputModel] {

    val i: IntOption = new IntOption()
    val l: LongOption = new LongOption()

    override def reset: Unit = {
      i.setNull()
      l.setNull()
    }

    override def copyFrom(other: ExtendOutputModel): Unit = {
      i.copyFrom(other.i)
      l.copyFrom(other.l)
    }

    def getIOption: IntOption = i
    def getLOption: LongOption = l
  }

  class RestructureInputModel extends DataModel[RestructureInputModel] {

    val i: IntOption = new IntOption()
    val l: LongOption = new LongOption()

    override def reset: Unit = {
      i.setNull()
      l.setNull()
    }

    override def copyFrom(other: RestructureInputModel): Unit = {
      i.copyFrom(other.i)
      l.copyFrom(other.l)
    }

    def getIOption: IntOption = i
    def getLOption: LongOption = l
  }

  class RestructureOutputModel extends DataModel[RestructureOutputModel] {

    val i: IntOption = new IntOption()
    val d: DoubleOption = new DoubleOption()

    override def reset: Unit = {
      i.setNull()
      d.setNull()
    }

    override def copyFrom(other: RestructureOutputModel): Unit = {
      i.copyFrom(other.i)
      d.copyFrom(other.d)
    }

    def getIOption: IntOption = i
    def getDOption: DoubleOption = d
  }
}
