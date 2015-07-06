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
package subplan

import org.junit.runner.RunWith
import org.scalatest.FlatSpec
import org.scalatest.junit.JUnitRunner

import java.io.{ DataInput, DataOutput, File }
import java.nio.file.{ Files, Path }

import scala.collection.mutable
import scala.collection.JavaConversions._
import scala.concurrent.{ Await, Future }
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.{ NullWritable, Writable }
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

import com.asakusafw.lang.compiler.api.CompilerOptions
import com.asakusafw.lang.compiler.api.testing.MockJobflowProcessorContext
import com.asakusafw.lang.compiler.model.description._
import com.asakusafw.lang.compiler.model.graph._
import com.asakusafw.lang.compiler.model.info.ExternalInputInfo
import com.asakusafw.lang.compiler.planning.{ PlanBuilder, PlanMarker }
import com.asakusafw.runtime.model.DataModel
import com.asakusafw.runtime.value._
import com.asakusafw.runtime.stage.input.TemporaryInputFormat
import com.asakusafw.spark.compiler.planning.{ SubPlanInfo, SubPlanOutputInfo }
import com.asakusafw.spark.compiler.spi.SubPlanCompiler
import com.asakusafw.spark.runtime.driver._
import com.asakusafw.spark.runtime.rdd.BranchKey
import com.asakusafw.spark.tools.asm._

@RunWith(classOf[JUnitRunner])
class InputOutputDriverClassBuilderSpecTest extends InputOutputDriverClassBuilderSpec

class InputOutputDriverClassBuilderSpec extends FlatSpec with SparkWithClassServerSugar with TempDir {

  import InputOutputDriverClassBuilderSpec._

  behavior of "Input/OutputDriverClassBuilder"

  it should "build input and output driver class" in {
    val tmpDir = new File(createTempDirectory("test-").toFile, "tmp")
    val path = tmpDir.getAbsolutePath

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
    outputSubPlan.putAttribute(classOf[SubPlanInfo],
      new SubPlanInfo(outputSubPlan, SubPlanInfo.DriverType.OUTPUT, Seq.empty[SubPlanInfo.DriverOption], outputOperator))

    val outputBranchKeysClassBuilder = new BranchKeysClassBuilder("outputFlowId")
    val outputBroadcastIdsClassBuilder = new BroadcastIdsClassBuilder("outputFlowId")
    val outputCompilerContext = SubPlanCompiler.Context(
      flowId = "outtputFlowId",
      jpContext = jpContext,
      externalInputs = mutable.Map.empty,
      branchKeys = outputBranchKeysClassBuilder,
      broadcastIds = outputBroadcastIdsClassBuilder)
    val outputCompiler = SubPlanCompiler(outputSubPlan.getAttribute(classOf[SubPlanInfo]).getDriverType)(outputCompilerContext)
    val outputDriverType = outputCompiler.compile(outputSubPlan)(outputCompilerContext)
    outputCompilerContext.jpContext.addClass(outputBranchKeysClassBuilder)
    outputCompilerContext.jpContext.addClass(outputBroadcastIdsClassBuilder)
    val outputDriverCls = classServer.loadClass(outputDriverType).asSubclass(classOf[OutputDriver[Hoge]])

    val hoges = sc.parallelize(0 until 10).map { i =>
      val hoge = new Hoge()
      hoge.id.modify(i)
      (hoge, hoge)
    }
    val terminators = mutable.Set.empty[Future[Unit]]
    val outputDriver = outputDriverCls.getConstructor(
      classOf[SparkContext],
      classOf[Broadcast[Configuration]],
      classOf[Seq[Future[RDD[_]]]],
      classOf[mutable.Set[Future[Unit]]])
      .newInstance(
        sc,
        hadoopConf,
        Seq(Future.successful(hoges)),
        terminators)
    outputDriver.execute()
    terminators.foreach {
      Await.result(_, Duration.Inf)
    }

    // prepare for input
    val srcDir = new File(tmpDir, s"${outputOperator.getName}")
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
    inputSubPlan.putAttribute(classOf[SubPlanInfo],
      new SubPlanInfo(inputSubPlan, SubPlanInfo.DriverType.INPUT, Seq.empty[SubPlanInfo.DriverOption], inputOperator))
    val inputSubplanOutput = inputSubPlan.getOutputs.find(_.getOperator.getOriginalSerialNumber == inputMarker.getOriginalSerialNumber).get
    inputSubplanOutput.putAttribute(classOf[SubPlanOutputInfo],
      new SubPlanOutputInfo(inputSubplanOutput, SubPlanOutputInfo.OutputType.DONT_CARE, Seq.empty[SubPlanOutputInfo.OutputOption], null, null))

    val inputBranchKeysClassBuilder = new BranchKeysClassBuilder("inputFlowId")
    val inputBroadcastIdsClassBuilder = new BroadcastIdsClassBuilder("inputFlowId")
    val inputCompilerContext = SubPlanCompiler.Context(
      flowId = "inputFlowId",
      jpContext = jpContext,
      externalInputs = mutable.Map.empty,
      branchKeys = inputBranchKeysClassBuilder,
      broadcastIds = inputBroadcastIdsClassBuilder)
    val inputCompiler = SubPlanCompiler(inputSubPlan.getAttribute(classOf[SubPlanInfo]).getDriverType)(inputCompilerContext)
    val inputDriverType = inputCompiler.compile(inputSubPlan)(inputCompilerContext)
    inputCompilerContext.jpContext.addClass(inputBranchKeysClassBuilder)
    inputCompilerContext.jpContext.addClass(inputBroadcastIdsClassBuilder)
    val inputDriverCls = classServer.loadClass(inputDriverType).asSubclass(classOf[InputDriver[NullWritable, Hoge, TemporaryInputFormat[Hoge]]])
    val inputDriver = inputDriverCls.getConstructor(
      classOf[SparkContext],
      classOf[Broadcast[Configuration]],
      classOf[Map[BroadcastId, Broadcast[_]]])
      .newInstance(
        sc,
        hadoopConf,
        Map.empty)
    val inputs = inputDriver.execute()

    val branchKeyCls = classServer.loadClass(inputBranchKeysClassBuilder.thisType.getClassName)
    def getBranchKey(osn: Long): BranchKey = {
      val sn = inputSubPlan.getOperators.toSet.find(_.getOriginalSerialNumber == osn).get.getSerialNumber
      branchKeyCls.getField(inputBranchKeysClassBuilder.getField(sn)).get(null).asInstanceOf[BranchKey]
    }

    assert(inputDriver.branchKeys ===
      Set(inputMarker)
      .map(marker => getBranchKey(marker.getOriginalSerialNumber)))

    assert(Await.result(inputs(getBranchKey(inputMarker.getOriginalSerialNumber))
      .map(_.map(_._2.asInstanceOf[Hoge].id.get)), Duration.Inf).collect.toSeq.sorted === (0 until 10))
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
