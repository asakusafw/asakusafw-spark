/*
 * Copyright 2011-2016 Asakusa Framework Team.
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
package com.asakusafw.spark.extensions.iterativebatch.compiler
package graph

import org.junit.runner.RunWith
import org.scalatest.fixture.FlatSpec
import org.scalatest.junit.JUnitRunner

import java.io.{ DataInput, DataOutput, File }

import scala.collection.JavaConversions._
import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{ NullWritable, Writable }
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.spark.SparkContext

import com.asakusafw.lang.compiler.api.CompilerOptions
import com.asakusafw.lang.compiler.api.testing.MockJobflowProcessorContext
import com.asakusafw.lang.compiler.common.DiagnosticException
import com.asakusafw.lang.compiler.hadoop.{ InputFormatInfo, InputFormatInfoExtension }
import com.asakusafw.lang.compiler.model.description.ClassDescription
import com.asakusafw.lang.compiler.model.graph.{ ExternalInput, MarkerOperator }
import com.asakusafw.lang.compiler.model.info.ExternalInputInfo
import com.asakusafw.lang.compiler.model.iterative.IterativeExtension
import com.asakusafw.lang.compiler.planning.{ PlanBuilder, PlanMarker }
import com.asakusafw.runtime.compatibility.JobCompatibility
import com.asakusafw.runtime.model.DataModel
import com.asakusafw.runtime.stage.StageConstants
import com.asakusafw.runtime.stage.input.TemporaryInputFormat
import com.asakusafw.runtime.stage.output.TemporaryOutputFormat
import com.asakusafw.runtime.value.IntOption
import com.asakusafw.spark.compiler.{ ClassServerForAll, FlowIdForEach }
import com.asakusafw.spark.compiler.graph._
import com.asakusafw.spark.compiler.planning.{ IterativeInfo, SubPlanInfo, SubPlanOutputInfo }
import com.asakusafw.spark.runtime.{ RoundContextSugar, TempDirForEach }
import com.asakusafw.spark.runtime.fixture.SparkForAll
import com.asakusafw.spark.runtime.graph.{
  Broadcast,
  BroadcastId,
  DirectInput,
  TemporaryInput
}
import com.asakusafw.spark.runtime.rdd.BranchKey

import com.asakusafw.spark.extensions.iterativebatch.compiler.spi.RoundAwareNodeCompiler
import com.asakusafw.spark.extensions.iterativebatch.runtime.graph.RoundAwareParallelCollectionSource

abstract class InputClassBuilderSpec extends FlatSpec with ClassServerForAll with SparkForAll {

  import InputClassBuilderSpec._

  def prepareRound(parent: String, ints: Seq[Int], round: Int): Unit = {
    val job = JobCompatibility.newJob(sc.hadoopConfiguration)
    job.setOutputKeyClass(classOf[NullWritable])
    job.setOutputValueClass(classOf[Foo])
    job.setOutputFormatClass(classOf[TemporaryOutputFormat[Foo]])

    TemporaryOutputFormat.setOutputPath(job, new Path(parent, s"foos_round_${round}"))

    val foos = sc.parallelize(ints).map(Foo.intToFoo(round))
    foos.saveAsNewAPIHadoopDataset(job.getConfiguration)
  }
}

@RunWith(classOf[JUnitRunner])
class TemporaryInputClassBuilderSpecTest extends TemporaryInputClassBuilderSpec

class TemporaryInputClassBuilderSpec
  extends InputClassBuilderSpec
  with FlowIdForEach
  with UsingCompilerContext
  with TempDirForEach
  with RoundContextSugar {

  import InputClassBuilderSpec._

  behavior of classOf[TemporaryInputClassBuilder].getSimpleName

  for {
    iterativeExtension <- Seq(
      Some(new IterativeExtension()),
      Some(new IterativeExtension("round")),
      None)
    iterativeInfo <- if (iterativeExtension.isDefined) {
      Seq(IterativeInfo.never())
    } else {
      Seq(IterativeInfo.always(), IterativeInfo.parameter("round"), IterativeInfo.never())
    }
  } {
    val conf = s"IterativeExtension: ${iterativeExtension}, IterativeInfo: ${iterativeInfo}"

    it should s"build TemporaryInput class: [${conf}]" in { implicit sc =>
      val tmpDir = createTempDirectoryForEach("test-").toFile.getAbsolutePath

      val beginMarker = MarkerOperator.builder(ClassDescription.of(classOf[Foo]))
        .attribute(classOf[PlanMarker], PlanMarker.BEGIN).build()
      val inputMarker = MarkerOperator.builder(ClassDescription.of(classOf[Foo]))
        .attribute(classOf[PlanMarker], PlanMarker.CHECKPOINT).build()

      val inputOperator = {
        val op = ExternalInput.builder(s"foos_${StageConstants.EXPR_STAGE_ID}/part-*",
          new ExternalInputInfo.Basic(
            ClassDescription.of(classOf[Foo]),
            "test",
            ClassDescription.of(classOf[Foo]),
            ExternalInputInfo.DataSize.UNKNOWN))
          .input("begin", ClassDescription.of(classOf[Foo]), beginMarker.getOutput)
          .output(ExternalInput.PORT_NAME, ClassDescription.of(classOf[Foo]))
        iterativeExtension.foreach(op.attribute(classOf[IterativeExtension], _))
        op.build()
      }
      inputOperator.findOutput(ExternalInput.PORT_NAME).connect(inputMarker.getInput)

      val plan = PlanBuilder.from(Seq(inputOperator))
        .add(
          Seq(beginMarker),
          Seq(inputMarker)).build().getPlan()
      assert(plan.getElements.size === 1)

      val subplan = plan.getElements.head
      subplan.putAttr(
        new SubPlanInfo(_,
          SubPlanInfo.DriverType.INPUT,
          Seq.empty[SubPlanInfo.DriverOption],
          inputOperator))
      subplan.putAttr(_ => iterativeInfo)

      subplan.findOut(inputMarker)
        .putAttr(
          new SubPlanOutputInfo(_,
            SubPlanOutputInfo.OutputType.DONT_CARE,
            Seq.empty[SubPlanOutputInfo.OutputOption], null, null))

      val jpContext = new MockJobflowProcessorContext(
        new CompilerOptions("buildid", tmpDir, Map.empty[String, String]),
        Thread.currentThread.getContextClassLoader,
        classServer.root.toFile)

      implicit val context = newNodeCompilerContext(flowId, jpContext)

      val compiler = RoundAwareNodeCompiler.get(subplan)

      if (iterativeExtension.isDefined) {
        intercept[DiagnosticException](compiler.compile(subplan))
      } else {
        val thisType = compiler.compile(subplan)
        context.addClass(context.branchKeys)
        context.addClass(context.broadcastIds)
        val cls = classServer.loadClass(thisType).asSubclass(classOf[TemporaryInput[Foo]])

        val branchKeyCls = classServer.loadClass(context.branchKeys.thisType.getClassName)
        def getBranchKey(marker: MarkerOperator): BranchKey = {
          val sn = subplan.getOperators.toSet
            .find(_.getOriginalSerialNumber == marker.getOriginalSerialNumber).get.getSerialNumber
          branchKeyCls.getField(context.branchKeys.getField(sn)).get(null).asInstanceOf[BranchKey]
        }

        val input = cls.getConstructor(
          classOf[Map[BroadcastId, Broadcast]],
          classOf[SparkContext])
          .newInstance(
            Map.empty,
            sc)

        assert(input.branchKeys === Set(inputMarker).map(getBranchKey))

        for {
          round <- 0 to 1
        } {
          prepareRound(new File(tmpDir, "external/input").getAbsolutePath, 0 until 100, round)

          val rc = newRoundContext(
            stageId = s"round_${round}",
            batchArguments = Map("round" -> round.toString))
          val bias = if (iterativeInfo.isIterative) 100 * round else 0

          val result = Await.result(
            input.getOrCompute(rc).apply(getBranchKey(inputMarker)).map {
              _().map {
                case (_, foo: Foo) => foo.id.get
              }.collect.toSeq.sorted
            }, Duration.Inf)

          assert(result === (0 until 100).map(i => bias + i))
        }
      }
    }
  }
}

@RunWith(classOf[JUnitRunner])
class DirectInputClassBuilderSpecTest extends DirectInputClassBuilderSpec

class DirectInputClassBuilderSpec
  extends InputClassBuilderSpec
  with FlowIdForEach
  with UsingCompilerContext
  with TempDirForEach
  with RoundContextSugar {

  import InputClassBuilderSpec._

  behavior of classOf[DirectInputClassBuilder].getSimpleName

  for {
    iterativeExtension <- Seq(
      Some(new IterativeExtension()),
      Some(new IterativeExtension("round")),
      None)
  } {
    val conf = s"IterativeExtension: ${iterativeExtension}"

    it should s"build DirectInput class: [${conf}]" in { implicit sc =>
      val tmpDir = createTempDirectoryForEach("test-").toFile.getAbsolutePath

      val beginMarker = MarkerOperator.builder(ClassDescription.of(classOf[Foo]))
        .attribute(classOf[PlanMarker], PlanMarker.BEGIN).build()
      val inputMarker = MarkerOperator.builder(ClassDescription.of(classOf[Foo]))
        .attribute(classOf[PlanMarker], PlanMarker.CHECKPOINT).build()

      val inputOperator = {
        val op = ExternalInput.builder( /*"foos_${round}"*/ "foos_0",
          new ExternalInputInfo.Basic(
            ClassDescription.of(classOf[Foo]),
            "test",
            ClassDescription.of(classOf[Foo]),
            ExternalInputInfo.DataSize.UNKNOWN))
          .input("begin", ClassDescription.of(classOf[Foo]), beginMarker.getOutput)
          .output(ExternalInput.PORT_NAME, ClassDescription.of(classOf[Foo]))
        iterativeExtension.foreach(op.attribute(classOf[IterativeExtension], _))
        op.build()
      }
      inputOperator.findOutput(ExternalInput.PORT_NAME).connect(inputMarker.getInput)

      val plan = PlanBuilder.from(Seq(inputOperator))
        .add(
          Seq(beginMarker),
          Seq(inputMarker)).build().getPlan()
      assert(plan.getElements.size === 1)

      val subplan = plan.getElements.head
      subplan.putAttr(
        new SubPlanInfo(_,
          SubPlanInfo.DriverType.INPUT,
          Seq.empty[SubPlanInfo.DriverOption],
          inputOperator))
      subplan.putAttr(_ => IterativeInfo.getDeclared(inputOperator))

      subplan.findOut(inputMarker)
        .putAttr(
          new SubPlanOutputInfo(_,
            SubPlanOutputInfo.OutputType.DONT_CARE,
            Seq.empty[SubPlanOutputInfo.OutputOption], null, null))

      val jpContext = new MockJobflowProcessorContext(
        new CompilerOptions("buildid", tmpDir, Map.empty[String, String]),
        Thread.currentThread.getContextClassLoader,
        classServer.root.toFile)
      jpContext.registerExtension(
        classOf[InputFormatInfoExtension],
        new InputFormatInfoExtension {

          override def resolve(name: String, info: ExternalInputInfo): InputFormatInfo = {
            assert(name === /*"foos_${round}"*/ "foos_0")
            assert(info.getModuleName === "test")
            new InputFormatInfo(
              ClassDescription.of(classOf[TemporaryInputFormat[Foo]]),
              ClassDescription.of(classOf[NullWritable]),
              ClassDescription.of(classOf[Foo]),
              mkExtraConfigurations(new File(tmpDir, /*"foos_round_${round}"*/ "foos_round_0").getAbsolutePath))
          }
        })

      implicit val context = newNodeCompilerContext(flowId, jpContext)

      val compiler = RoundAwareNodeCompiler.get(subplan)
      val thisType = compiler.compile(subplan)
      context.addClass(context.branchKeys)
      context.addClass(context.broadcastIds)
      val cls = classServer.loadClass(thisType).asSubclass(classOf[DirectInput[_, _, _]])

      val branchKeyCls = classServer.loadClass(context.branchKeys.thisType.getClassName)
      def getBranchKey(marker: MarkerOperator): BranchKey = {
        val sn = subplan.getOperators.toSet
          .find(_.getOriginalSerialNumber == marker.getOriginalSerialNumber).get.getSerialNumber
        branchKeyCls.getField(context.branchKeys.getField(sn)).get(null).asInstanceOf[BranchKey]
      }

      val input = cls.getConstructor(
        classOf[Map[BroadcastId, Broadcast]],
        classOf[SparkContext])
        .newInstance(
          Map.empty,
          sc)

      assert(input.branchKeys === Set(inputMarker).map(getBranchKey))

      for {
        round <- 0 to 0 // 1
      } {
        prepareRound(tmpDir, 0 until 100, round)

        val rc = newRoundContext(
          stageId = s"round_${round}",
          batchArguments = Map("round" -> round.toString))
        val bias = if (iterativeExtension.isDefined) 100 * round else 0

        val result = Await.result(
          input.getOrCompute(rc).apply(getBranchKey(inputMarker)).map {
            _().map {
              case (_, foo: Foo) => foo.id.get
            }.collect.toSeq.sorted
          }, Duration.Inf)

        assert(result === (0 until 100).map(i => bias + i))
      }
    }
  }

  private def mkExtraConfigurations(path: String): Map[String, String] = {
    val job = JobCompatibility.newJob(new Configuration(false))
    FileInputFormat.setInputPaths(job, new Path(path + "/part-*"))
    Map(FileInputFormat.INPUT_DIR -> job.getConfiguration.get(FileInputFormat.INPUT_DIR))
  }
}

object InputClassBuilderSpec {

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

  object Foo {

    def intToFoo(round: Int): Int => (NullWritable, Foo) = {

      lazy val foo = new Foo()

      { i =>
        foo.id.modify(100 * round + i)
        (NullWritable.get, foo)
      }
    }
  }
}
