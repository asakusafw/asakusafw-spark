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
import java.nio.file.Files

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
import com.asakusafw.lang.compiler.model.description.ClassDescription
import com.asakusafw.lang.compiler.model.graph.{ ExternalInput, ExternalOutput, MarkerOperator }
import com.asakusafw.lang.compiler.model.info.ExternalInputInfo
import com.asakusafw.lang.compiler.planning.{ PlanBuilder, PlanMarker }
import com.asakusafw.runtime.model.DataModel
import com.asakusafw.runtime.value.IntOption
import com.asakusafw.runtime.stage.input.TemporaryInputFormat
import com.asakusafw.spark.compiler.planning.{ SubPlanInfo, SubPlanOutputInfo }
import com.asakusafw.spark.compiler.spi.SubPlanCompiler
import com.asakusafw.spark.runtime.{ HadoopConfForEach, TempDirForEach }
import com.asakusafw.spark.runtime.driver.{ BroadcastId, InputDriver, OutputDriver }
import com.asakusafw.spark.runtime.rdd.BranchKey
import com.asakusafw.spark.tools.asm._

@RunWith(classOf[JUnitRunner])
class InputOutputDriverClassBuilderSpecTest extends InputOutputDriverClassBuilderSpec

class InputOutputDriverClassBuilderSpec
  extends FlatSpec
  with SparkWithClassServerForAll
  with HadoopConfForEach
  with TempDirForEach
  with UsingCompilerContext {

  import InputOutputDriverClassBuilderSpec._

  behavior of "Input/OutputDriverClassBuilder"

  it should "build input and output driver class" in {
    val tmpDir = new File(createTempDirectoryForEach("test-").toFile, "tmp")
    val path = tmpDir.getAbsolutePath

    val jpContext = new MockJobflowProcessorContext(
      new CompilerOptions("buildid", path, Map.empty[String, String]),
      Thread.currentThread.getContextClassLoader,
      classServer.root.toFile)

    // output
    val outputMarker = MarkerOperator.builder(ClassDescription.of(classOf[Foo]))
      .attribute(classOf[PlanMarker], PlanMarker.GATHER).build()
    val endMarker = MarkerOperator.builder(ClassDescription.of(classOf[Foo]))
      .attribute(classOf[PlanMarker], PlanMarker.END).build()

    val outputOperator = ExternalOutput.builder("foo")
      .input(ExternalOutput.PORT_NAME, ClassDescription.of(classOf[Foo]), outputMarker.getOutput)
      .output("end", ClassDescription.of(classOf[Foo]))
      .build()
    outputOperator.findOutput("end").connect(endMarker.getInput)

    val outputPlan = PlanBuilder.from(Seq(outputOperator))
      .add(
        Seq(outputMarker),
        Seq(endMarker)).build().getPlan()
    assert(outputPlan.getElements.size === 1)

    val outputSubPlan = outputPlan.getElements.head
    outputSubPlan.putAttr(
      new SubPlanInfo(_,
        SubPlanInfo.DriverType.OUTPUT,
        Seq.empty[SubPlanInfo.DriverOption],
        outputOperator))

    // FIXME: should use the same flowId as StageInfo's.
    val outputCompilerContext = newSubPlanCompilerContext("outtputFlowId", jpContext)

    val outputCompiler =
      SubPlanCompiler(
        outputSubPlan.getAttribute(classOf[SubPlanInfo]).getDriverType)(
          outputCompilerContext)
    val outputDriverType = outputCompiler.compile(outputSubPlan)(outputCompilerContext)
    outputCompilerContext.addClass(outputCompilerContext.branchKeys)
    outputCompilerContext.addClass(outputCompilerContext.broadcastIds)
    val outputDriverCls = classServer.loadClass(outputDriverType).asSubclass(classOf[OutputDriver[Foo]])

    val foos = sc.parallelize(0 until 10).map {

      lazy val foo = new Foo()

      { i =>
        foo.id.modify(i)
        (foo, foo)
      }
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
        Seq(Future.successful(foos)),
        terminators)
    outputDriver.execute()
    Await.result(Future.sequence(terminators), Duration.Inf)

    // prepare for input
    val srcDir = new File(tmpDir, s"${outputOperator.getName}")
    val dstDir = new File(tmpDir, s"${MockJobflowProcessorContext.EXTERNAL_INPUT_BASE}/${outputOperator.getName}")
    dstDir.getParentFile.mkdirs
    Files.move(srcDir.toPath, dstDir.toPath)

    // input
    val beginMarker = MarkerOperator.builder(ClassDescription.of(classOf[Foo]))
      .attribute(classOf[PlanMarker], PlanMarker.BEGIN).build()
    val inputMarker = MarkerOperator.builder(ClassDescription.of(classOf[Foo]))
      .attribute(classOf[PlanMarker], PlanMarker.CHECKPOINT).build()

    val inputOperator = ExternalInput.builder("foo/part-*",
      new ExternalInputInfo.Basic(
        ClassDescription.of(classOf[Foo]),
        "test",
        ClassDescription.of(classOf[Foo]),
        ExternalInputInfo.DataSize.UNKNOWN))
      .input("begin", ClassDescription.of(classOf[Foo]), beginMarker.getOutput)
      .output(ExternalInput.PORT_NAME, ClassDescription.of(classOf[Foo])).build()
    inputOperator.findOutput(ExternalInput.PORT_NAME).connect(inputMarker.getInput)

    val inputPlan = PlanBuilder.from(Seq(inputOperator))
      .add(
        Seq(beginMarker),
        Seq(inputMarker)).build().getPlan()
    assert(inputPlan.getElements.size === 1)

    val inputSubPlan = inputPlan.getElements.head
    inputSubPlan.putAttr(
      new SubPlanInfo(_,
        SubPlanInfo.DriverType.INPUT,
        Seq.empty[SubPlanInfo.DriverOption],
        inputOperator))

    inputSubPlan.findOut(inputMarker)
      .putAttr(
        new SubPlanOutputInfo(_,
          SubPlanOutputInfo.OutputType.DONT_CARE,
          Seq.empty[SubPlanOutputInfo.OutputOption], null, null))

    // FIXME: should use the same flowId as StageInfo's.
    val inputCompilerContext = newSubPlanCompilerContext("inputFlowId", jpContext)

    val inputCompiler =
      SubPlanCompiler(
        inputSubPlan.getAttribute(classOf[SubPlanInfo]).getDriverType)(
          inputCompilerContext)
    val inputDriverType = inputCompiler.compile(inputSubPlan)(inputCompilerContext)
    inputCompilerContext.addClass(inputCompilerContext.branchKeys)
    inputCompilerContext.addClass(inputCompilerContext.broadcastIds)
    val inputDriverCls = classServer.loadClass(inputDriverType).asSubclass(classOf[InputDriver[NullWritable, Foo, TemporaryInputFormat[Foo]]])
    val inputDriver = inputDriverCls.getConstructor(
      classOf[SparkContext],
      classOf[Broadcast[Configuration]],
      classOf[Map[BroadcastId, Broadcast[_]]])
      .newInstance(
        sc,
        hadoopConf,
        Map.empty)
    val inputs = inputDriver.execute()

    val branchKeyCls = classServer.loadClass(inputCompilerContext.branchKeys.thisType.getClassName)
    def getBranchKey(marker: MarkerOperator): BranchKey = {
      val sn = inputSubPlan.getOperators.toSet
        .find(_.getOriginalSerialNumber == marker.getOriginalSerialNumber).get.getSerialNumber
      branchKeyCls.getField(inputCompilerContext.branchKeys.getField(sn)).get(null).asInstanceOf[BranchKey]
    }

    assert(inputDriver.branchKeys === Set(inputMarker).map(getBranchKey))

    val result = Await.result(
      inputs(getBranchKey(inputMarker)).map {
        _.map {
          case (_, foo: Foo) => foo.id.get
        }.collect.toSeq.sorted
      }, Duration.Inf)

    assert(result === (0 until 10))
  }
}

object InputOutputDriverClassBuilderSpec {

  class Foo extends DataModel[Foo] with Writable {

    val id = new IntOption()

    override def reset(): Unit = {
      id.setNull()
    }
    override def copyFrom(other: Foo): Unit = {
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
