package com.asakusafw.spark.compiler
package subplan

import org.junit.runner.RunWith
import org.scalatest.FlatSpec
import org.scalatest.junit.JUnitRunner

import java.io.{ DataInput, DataOutput, File }
import java.nio.file.{ Files, Path }

import scala.collection.JavaConversions._

import org.apache.hadoop.io.Writable
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import com.asakusafw.lang.compiler.api.CompilerOptions
import com.asakusafw.lang.compiler.api.mock.MockJobflowProcessorContext
import com.asakusafw.lang.compiler.model.description._
import com.asakusafw.lang.compiler.model.graph._
import com.asakusafw.lang.compiler.model.info.ExternalInputInfo
import com.asakusafw.lang.compiler.planning.{ PlanBuilder, PlanMarker }
import com.asakusafw.lang.compiler.planning.spark.DominantOperator
import com.asakusafw.runtime.model.DataModel
import com.asakusafw.runtime.value._
import com.asakusafw.spark.compiler.spi.SubPlanCompiler
import com.asakusafw.spark.compiler.subplan.SubPlanType.{ InputSubPlan, OutputSubPlan }
import com.asakusafw.spark.runtime.driver._
import com.asakusafw.spark.tools.asm._

@RunWith(classOf[JUnitRunner])
class InputOutputDriverClassBuilderSpecTest extends InputOutputDriverClassBuilderSpec

class InputOutputDriverClassBuilderSpec extends FlatSpec with SparkWithClassServerSugar {

  import InputOutputDriverClassBuilderSpec._

  behavior of "Input/OutputDriverClassBuilder"

  it should "build input and output driver class" in {
    val tmpDir = File.createTempFile("test-", null)
    tmpDir.delete
    val path = tmpDir.getAbsolutePath

    val resolvers = SubPlanCompiler(Thread.currentThread.getContextClassLoader)

    val jpContext = new MockJobflowProcessorContext(
      new CompilerOptions("buildid", path, Map.empty[String, String]),
      Thread.currentThread.getContextClassLoader,
      classServer.root.toFile)

    // output
    val outputMarker = MarkerOperator.builder(ClassDescription.of(classOf[Hoge]))
      .attribute(classOf[PlanMarker], PlanMarker.GATHER).build()
    val endMarker = MarkerOperator.builder(ClassDescription.of(classOf[Hoge]))
      .attribute(classOf[PlanMarker], PlanMarker.END).build()

    val outputOperator = ExternalOutput.builder("hoge")
      .input(ExternalOutput.PORT_NAME, ClassDescription.of(classOf[Hoge]), outputMarker.getOutput)
      .output("end", ClassDescription.of(classOf[Hoge]))
      .build()
    outputOperator.findOutput("end").connect(endMarker.getInput)

    val outputPlan = PlanBuilder.from(Seq(outputOperator))
      .add(
        Seq(outputMarker),
        Seq(endMarker)).build().getPlan()
    assert(outputPlan.getElements.size === 1)
    val outputSubPlan = outputPlan.getElements.head
    outputSubPlan.putAttribute(classOf[DominantOperator], new DominantOperator(outputOperator))

    val outputCompiler = resolvers(outputOperator)
    val outputCompilerContext = outputCompiler.Context(flowId = "flowId", jpContext = jpContext)
    val outputDriverType = outputCompiler.compile(outputSubPlan)(outputCompilerContext)

    val outputDriverCls = classServer.loadClass(outputDriverType).asSubclass(classOf[OutputDriver[Hoge]])

    val hoges = sc.parallelize(0 until 10).map { i =>
      val hoge = new Hoge()
      hoge.id.modify(i)
      (hoge, hoge)
    }
    val outputDriver = outputDriverCls.getConstructor(classOf[SparkContext], classOf[RDD[_]])
      .newInstance(sc, hoges)
    outputDriver.execute()

    // prepare for input
    val sn = outputOperator.getSerialNumber
    val srcDir = new File(tmpDir, s"${outputOperator.getName}/${sn}")
    val dstDir = new File(tmpDir, s"${MockJobflowProcessorContext.EXTERNAL_INPUT_BASE}/${outputOperator.getName}")
    dstDir.getParentFile.mkdirs
    Files.move(srcDir.toPath, dstDir.toPath)

    // input
    val beginMarker = MarkerOperator.builder(ClassDescription.of(classOf[Hoge]))
      .attribute(classOf[PlanMarker], PlanMarker.BEGIN).build()
    val inputMarker = MarkerOperator.builder(ClassDescription.of(classOf[Hoge]))
      .attribute(classOf[PlanMarker], PlanMarker.CHECKPOINT).build()

    val inputOperator = ExternalInput.builder("hoge/part-*",
      new ExternalInputInfo.Basic(
        ClassDescription.of(classOf[Hoge]),
        "test",
        ClassDescription.of(classOf[Hoge]),
        ExternalInputInfo.DataSize.UNKNOWN))
      .input("begin", ClassDescription.of(classOf[Hoge]), beginMarker.getOutput)
      .output(ExternalInput.PORT_NAME, ClassDescription.of(classOf[Hoge])).build()
    inputOperator.findOutput(ExternalInput.PORT_NAME).connect(inputMarker.getInput)

    val inputPlan = PlanBuilder.from(Seq(inputOperator))
      .add(
        Seq(beginMarker),
        Seq(inputMarker)).build().getPlan()
    assert(inputPlan.getElements.size === 1)
    val inputSubPlan = inputPlan.getElements.head
    inputSubPlan.putAttribute(classOf[DominantOperator], new DominantOperator(inputOperator))

    val inputCompiler = resolvers(inputOperator)
    val inputCompilerContext = inputCompiler.Context(flowId = "flowId", jpContext = jpContext)
    val inputDriverType = inputCompiler.compile(inputSubPlan)(inputCompilerContext)

    val inputDriverCls = classServer.loadClass(inputDriverType).asSubclass(classOf[InputDriver[Hoge, Long]])
    val inputDriver = inputDriverCls.getConstructor(classOf[SparkContext]).newInstance(sc)
    val inputs = inputDriver.execute()
    assert(inputDriver.branchKey === inputMarker.getOriginalSerialNumber)
    assert(inputs(inputMarker.getOriginalSerialNumber).map(_._2.asInstanceOf[Hoge].id.get).collect.toSeq === (0 until 10))
  }
}

object InputOutputDriverClassBuilderSpec {

  class Hoge extends DataModel[Hoge] with Writable {

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
}
