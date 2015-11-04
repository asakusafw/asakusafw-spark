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

import scala.collection.JavaConversions._
import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{ NullWritable, Writable }
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast

import com.asakusafw.lang.compiler.api.CompilerOptions
import com.asakusafw.lang.compiler.api.testing.MockJobflowProcessorContext
import com.asakusafw.lang.compiler.model.description.ClassDescription
import com.asakusafw.lang.compiler.model.graph.{ ExternalInput, MarkerOperator }
import com.asakusafw.lang.compiler.model.info.ExternalInputInfo
import com.asakusafw.lang.compiler.planning.{ PlanBuilder, PlanMarker }
import com.asakusafw.runtime.compatibility.JobCompatibility
import com.asakusafw.runtime.model.DataModel
import com.asakusafw.runtime.stage.input.TemporaryInputFormat
import com.asakusafw.runtime.stage.output.TemporaryOutputFormat
import com.asakusafw.runtime.value.IntOption
import com.asakusafw.spark.compiler.planning.{ SubPlanInfo, SubPlanOutputInfo }
import com.asakusafw.spark.compiler.spi.SubPlanCompiler
import com.asakusafw.spark.runtime.{ HadoopConfForEach, TempDirForEach }
import com.asakusafw.spark.runtime.driver.{ BroadcastId, InputDriver }
import com.asakusafw.spark.runtime.rdd.BranchKey
import com.asakusafw.spark.tools.asm._

@RunWith(classOf[JUnitRunner])
class InputDriverClassBuilderSpecTest extends InputDriverClassBuilderSpec

class InputDriverClassBuilderSpec
  extends FlatSpec
  with SparkWithClassServerForAll
  with HadoopConfForEach
  with TempDirForEach
  with UsingCompilerContext {

  import InputDriverClassBuilderSpec._

  behavior of classOf[InputDriverClassBuilder].getSimpleName

  def prepareInput(path: String, ints: Seq[Int]): Unit = {
    val job = JobCompatibility.newJob(sc.hadoopConfiguration)
    job.setOutputKeyClass(classOf[NullWritable])
    job.setOutputValueClass(classOf[Foo])
    job.setOutputFormatClass(classOf[TemporaryOutputFormat[Foo]])

    TemporaryOutputFormat.setOutputPath(job, new Path(path))

    val foos = sc.parallelize(ints).map(Foo.intToFoo)
    foos.saveAsNewAPIHadoopDataset(job.getConfiguration)
  }

  it should "build input driver class" in {
    val tmpDir = new File(createTempDirectoryForEach("test-").toFile, "tmp").getAbsolutePath

    val beginMarker = MarkerOperator.builder(ClassDescription.of(classOf[Foo]))
      .attribute(classOf[PlanMarker], PlanMarker.BEGIN).build()
    val inputMarker = MarkerOperator.builder(ClassDescription.of(classOf[Foo]))
      .attribute(classOf[PlanMarker], PlanMarker.CHECKPOINT).build()

    val inputOperator = ExternalInput.builder("foos/part-*",
      new ExternalInputInfo.Basic(
        ClassDescription.of(classOf[Foo]),
        "test",
        ClassDescription.of(classOf[Foo]),
        ExternalInputInfo.DataSize.UNKNOWN))
      .input("begin", ClassDescription.of(classOf[Foo]), beginMarker.getOutput)
      .output(ExternalInput.PORT_NAME, ClassDescription.of(classOf[Foo])).build()
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

    subplan.findOut(inputMarker)
      .putAttr(
        new SubPlanOutputInfo(_,
          SubPlanOutputInfo.OutputType.DONT_CARE,
          Seq.empty[SubPlanOutputInfo.OutputOption], null, null))

    val jpContext = new MockJobflowProcessorContext(
      new CompilerOptions("buildid", tmpDir, Map.empty[String, String]),
      Thread.currentThread.getContextClassLoader,
      classServer.root.toFile)

    implicit val context = newSubPlanCompilerContext(flowId, jpContext)

    val compiler = SubPlanCompiler(subplan.getAttribute(classOf[SubPlanInfo]).getDriverType)
    val thisType = compiler.compile(subplan)
    context.addClass(context.branchKeys)
    context.addClass(context.broadcastIds)
    val cls = classServer.loadClass(thisType)
      .asSubclass(classOf[InputDriver[NullWritable, Foo, TemporaryInputFormat[Foo]]])
    val driver = cls.getConstructor(
      classOf[SparkContext],
      classOf[Broadcast[Configuration]],
      classOf[Map[BroadcastId, Broadcast[_]]])
      .newInstance(
        sc,
        hadoopConf,
        Map.empty)

    val branchKeyCls = classServer.loadClass(context.branchKeys.thisType.getClassName)
    def getBranchKey(marker: MarkerOperator): BranchKey = {
      val sn = subplan.getOperators.toSet
        .find(_.getOriginalSerialNumber == marker.getOriginalSerialNumber).get.getSerialNumber
      branchKeyCls.getField(context.branchKeys.getField(sn)).get(null).asInstanceOf[BranchKey]
    }

    assert(driver.branchKeys === Set(inputMarker).map(getBranchKey))

    prepareInput(new File(tmpDir, "external/input/foos").getAbsolutePath, 0 until 10)

    val inputs = driver.execute()
    val result = Await.result(
      inputs(getBranchKey(inputMarker)).map {
        _.map {
          case (_, foo: Foo) => foo.id.get
        }.collect.toSeq.sorted
      }, Duration.Inf)

    assert(result === (0 until 10))
  }
}

object InputDriverClassBuilderSpec {

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

    def intToFoo: Int => (NullWritable, Foo) = {

      lazy val foo = new Foo()

      { i =>
        foo.id.modify(i)
        (NullWritable.get, foo)
      }
    }
  }
}
