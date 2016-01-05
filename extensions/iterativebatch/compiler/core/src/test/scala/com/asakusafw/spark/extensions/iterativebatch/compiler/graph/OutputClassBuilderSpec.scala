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

import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{ NullWritable, Writable }
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.spark.SparkContext

import com.asakusafw.bridge.stage.StageInfo
import com.asakusafw.lang.compiler.api.CompilerOptions
import com.asakusafw.lang.compiler.api.testing.MockJobflowProcessorContext
import com.asakusafw.lang.compiler.model.description.ClassDescription
import com.asakusafw.lang.compiler.model.graph.{ ExternalOutput, MarkerOperator }
import com.asakusafw.lang.compiler.planning.{ PlanBuilder, PlanMarker }
import com.asakusafw.runtime.compatibility.JobCompatibility
import com.asakusafw.runtime.model.DataModel
import com.asakusafw.runtime.stage.input.TemporaryInputFormat
import com.asakusafw.runtime.value.IntOption
import com.asakusafw.spark.compiler.FlowIdForEach
import com.asakusafw.spark.compiler.fixture.SparkWithClassServerForAll
import com.asakusafw.spark.compiler.graph._
import com.asakusafw.spark.compiler.planning.SubPlanInfo
import com.asakusafw.spark.runtime.{ RoundContext, RoundContextSugar, TempDirForEach }
import com.asakusafw.spark.runtime.graph.{
  Broadcast,
  BroadcastId,
  ParallelCollectionSource,
  Source,
  TemporaryOutput
}
import com.asakusafw.spark.runtime.rdd.BranchKey

import com.asakusafw.spark.extensions.iterativebatch.compiler.spi.RoundAwareNodeCompiler
import com.asakusafw.spark.extensions.iterativebatch.runtime.graph.RoundAwareParallelCollectionSource

abstract class OutputClassBuilderSpec extends FlatSpec with SparkWithClassServerForAll {

  import OutputClassBuilderSpec._

  def readResult(path: String, rc: RoundContext): Seq[Int] = {
    val job = JobCompatibility.newJob(rc.hadoopConf.value)

    val stageInfo = StageInfo.deserialize(job.getConfiguration.get(StageInfo.KEY_NAME))
    FileInputFormat.setInputPaths(
      job,
      new Path(stageInfo.resolveVariables(s"${path}/${stageInfo.getStageId}/part-*")))

    sc.newAPIHadoopRDD(
      job.getConfiguration,
      classOf[TemporaryInputFormat[Foo]],
      classOf[NullWritable],
      classOf[Foo]).map(_._2.id.get).collect.toSeq.sorted
  }
}

@RunWith(classOf[JUnitRunner])
class TemporaryOutputClassBuilderSpecTest extends TemporaryOutputClassBuilderSpec

class TemporaryOutputClassBuilderSpec
  extends OutputClassBuilderSpec
  with FlowIdForEach
  with UsingCompilerContext
  with TempDirForEach
  with RoundContextSugar {

  import OutputClassBuilderSpec._

  behavior of classOf[TemporaryOutputClassBuilder].getSimpleName

  it should "build TemporaryOutput class" in { implicit sc =>
    val tmpDir = createTempDirectoryForEach("test-").toFile.getAbsolutePath

    val foosMarker = MarkerOperator.builder(ClassDescription.of(classOf[Foo]))
      .attribute(classOf[PlanMarker], PlanMarker.GATHER).build()
    val endMarker = MarkerOperator.builder(ClassDescription.of(classOf[Foo]))
      .attribute(classOf[PlanMarker], PlanMarker.END).build()

    val outputOperator = ExternalOutput.builder("foos")
      .input(ExternalOutput.PORT_NAME, ClassDescription.of(classOf[Foo]), foosMarker.getOutput)
      .output("end", ClassDescription.of(classOf[Foo]))
      .build()
    outputOperator.findOutput("end").connect(endMarker.getInput)

    val plan = PlanBuilder.from(Seq(outputOperator))
      .add(
        Seq(foosMarker),
        Seq(endMarker)).build().getPlan()
    assert(plan.getElements.size === 1)

    val subplan = plan.getElements.head
    subplan.putAttr(
      new SubPlanInfo(_,
        SubPlanInfo.DriverType.OUTPUT,
        Seq.empty[SubPlanInfo.DriverOption],
        outputOperator))

    val foosInput = subplan.findIn(foosMarker)

    val jpContext = new MockJobflowProcessorContext(
      new CompilerOptions("buildid", tmpDir, Map.empty[String, String]),
      Thread.currentThread.getContextClassLoader,
      classServer.root.toFile)

    implicit val context = newNodeCompilerContext(flowId, jpContext)
    context.branchKeys.getField(foosInput.getOperator.getSerialNumber)

    val compiler = RoundAwareNodeCompiler.get(subplan)
    val thisType = compiler.compile(subplan)
    context.addClass(context.branchKeys)
    context.addClass(context.broadcastIds)
    val cls = classServer.loadClass(thisType).asSubclass(classOf[TemporaryOutput[Foo]])

    val branchKeyCls = classServer.loadClass(context.branchKeys.thisType.getClassName)
    def getBranchKey(marker: MarkerOperator): BranchKey = {
      val sn = subplan.getOperators.toSet
        .find(_.getOriginalSerialNumber == marker.getOriginalSerialNumber).get.getSerialNumber
      branchKeyCls.getField(context.branchKeys.getField(sn)).get(null).asInstanceOf[BranchKey]
    }

    val source =
      new RoundAwareParallelCollectionSource(Input, (0 until 100))("input")
        .mapWithRoundContext(Input)(Foo.intToFoo)
    val output = cls.getConstructor(
      classOf[Seq[(Source, BranchKey)]],
      classOf[SparkContext])
      .newInstance(
        Seq((source, getBranchKey(foosMarker))),
        sc)

    val path = new File(tmpDir, outputOperator.getName).getAbsolutePath

    for {
      round <- 0 to 1
    } {
      val rc = newRoundContext(
        stageId = s"round_${round}",
        batchArguments = Map("round" -> round.toString))

      Await.result(output.submitJob(rc), Duration.Inf)

      val result = readResult(path, rc)
      assert(result.size === 100)
      assert(result === (0 until 100).map(i => 100 * round + i))
    }
  }
}

object OutputClassBuilderSpec {

  val Input = BranchKey(0)

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

    def intToFoo(rc: RoundContext): Int => (_, Foo) = {

      val stageInfo = StageInfo.deserialize(rc.hadoopConf.value.get(StageInfo.KEY_NAME))
      val round = stageInfo.getBatchArguments()("round").toInt

      lazy val foo = new Foo()

      { i =>
        foo.id.modify(100 * round + i)
        (NullWritable.get, foo)
      }
    }
  }
}
