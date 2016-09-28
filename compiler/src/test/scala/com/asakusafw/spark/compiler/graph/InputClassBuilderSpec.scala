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
package com.asakusafw.spark.compiler
package graph

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
import org.apache.hadoop.mapreduce.{ InputFormat, Job => MRJob }
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.{ FileOutputFormat, SequenceFileOutputFormat }

import com.asakusafw.bridge.hadoop.directio.DirectFileInputFormat
import com.asakusafw.lang.compiler.api.CompilerOptions
import com.asakusafw.lang.compiler.api.testing.MockJobflowProcessorContext
import com.asakusafw.lang.compiler.hadoop.{ InputFormatInfo, InputFormatInfoExtension }
import com.asakusafw.lang.compiler.model.description.ClassDescription
import com.asakusafw.lang.compiler.model.graph.{ ExternalInput, MarkerOperator }
import com.asakusafw.lang.compiler.model.info.ExternalInputInfo
import com.asakusafw.lang.compiler.planning.{ PlanBuilder, PlanMarker }
import com.asakusafw.runtime.directio.hadoop.{ HadoopDataSource, SequenceFileFormat }
import com.asakusafw.runtime.model.DataModel
import com.asakusafw.runtime.stage.input.TemporaryInputFormat
import com.asakusafw.runtime.stage.output.TemporaryOutputFormat
import com.asakusafw.runtime.value.IntOption
import com.asakusafw.spark.compiler.planning.{ SubPlanInfo, SubPlanOutputInfo }
import com.asakusafw.spark.compiler.spi.NodeCompiler
import com.asakusafw.spark.runtime._
import com.asakusafw.spark.runtime.JobContext.InputCounter
import com.asakusafw.spark.runtime.graph.{
  Broadcast,
  BroadcastId,
  DirectInput,
  TemporaryInput
}
import com.asakusafw.spark.runtime.rdd.BranchKey

abstract class InputClassBuilderSpec extends FlatSpec with ClassServerForAll with SparkForAll {

  import InputClassBuilderSpec._

  def prepareInput(
    path: String,
    configurePath: (MRJob, String) => Unit)(
      ints: Seq[Int]): Unit = {
    val job = MRJob.getInstance(sc.hadoopConfiguration)
    job.setOutputKeyClass(classOf[NullWritable])
    job.setOutputValueClass(classOf[Foo])
    configurePath(job, path)
    val foos = sc.parallelize(ints).map(Foo.intToFoo)
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
  with JobContextSugar
  with RoundContextSugar {

  import InputClassBuilderSpec._

  behavior of classOf[TemporaryInputClassBuilder].getSimpleName

  val configurePath: (MRJob, String) => Unit = { (job, path) =>
    job.setOutputFormatClass(classOf[TemporaryOutputFormat[Foo]])
    TemporaryOutputFormat.setOutputPath(job, new Path(path))
  }

  it should "build TemporaryInput class" in {
    val tmpDir = createTempDirectoryForEach("test-").toFile.getAbsolutePath

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

    implicit val context = newNodeCompilerContext(flowId, jpContext)

    val compiler = NodeCompiler.get(subplan)
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

    implicit val jobContext = newJobContext(sc)

    val input = cls.getConstructor(
      classOf[Map[BroadcastId, Broadcast[_]]],
      classOf[JobContext])
      .newInstance(
        Map.empty,
        jobContext)

    assert(input.branchKeys === Set(inputMarker).map(getBranchKey))

    prepareInput(new File(tmpDir, "external/input/foos").getAbsolutePath, configurePath)(0 until 100)

    val rc = newRoundContext()

    val result = Await.result(
      input.compute(rc).apply(getBranchKey(inputMarker)).map {
        _.map {
          case (_, foo: Foo) => foo.id.get
        }.collect.toSeq.sorted
      }, Duration.Inf)

    assert(result === (0 until 100))

    assert(jobContext.inputStatistics(InputCounter.External).size === 1)
    val statistics = jobContext.inputStatistics(InputCounter.External)(inputOperator.getName)
    assert(statistics.records === 100)
  }
}

@RunWith(classOf[JUnitRunner])
class DirectInputClassBuilderSpecTest extends DirectInputClassBuilderSpec

class DirectInputClassBuilderSpec
  extends InputClassBuilderSpec
  with FlowIdForEach
  with UsingCompilerContext
  with TempDirForEach
  with JobContextSugar
  with RoundContextSugar {

  import InputClassBuilderSpec._

  behavior of classOf[DirectInputClassBuilder].getSimpleName

  val configurePath: (MRJob, String) => Unit = { (job, path) =>
    job.setOutputFormatClass(classOf[SequenceFileOutputFormat[NullWritable, Foo]])
    FileOutputFormat.setOutputPath(job, new Path(path))
  }

  it should "build DirectInput class" in {
    val tmpDir = createTempDirectoryForEach("test-").toFile.getAbsolutePath

    val beginMarker = MarkerOperator.builder(ClassDescription.of(classOf[Foo]))
      .attribute(classOf[PlanMarker], PlanMarker.BEGIN).build()
    val inputMarker = MarkerOperator.builder(ClassDescription.of(classOf[Foo]))
      .attribute(classOf[PlanMarker], PlanMarker.CHECKPOINT).build()

    val inputOperator = ExternalInput.builder("foos",
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
    jpContext.registerExtension(
      classOf[InputFormatInfoExtension],
      new InputFormatInfoExtension {

        override def resolve(name: String, info: ExternalInputInfo): InputFormatInfo = {
          assert(name === "foos")
          assert(info.getModuleName === "test")
          new InputFormatInfo(
            ClassDescription.of(classOf[DirectFileInputFormat]),
            ClassDescription.of(classOf[NullWritable]),
            ClassDescription.of(classOf[Foo]),
            Map(
              "com.asakusafw.directio.test" -> classOf[HadoopDataSource].getName,
              "com.asakusafw.directio.test.path" -> "test",
              "com.asakusafw.directio.test.fs.path" -> tmpDir,
              DirectFileInputFormat.KEY_BASE_PATH -> "test",
              DirectFileInputFormat.KEY_RESOURCE_PATH -> s"foos*/part-*",
              DirectFileInputFormat.KEY_DATA_CLASS -> classOf[Foo].getName,
              DirectFileInputFormat.KEY_FORMAT_CLASS -> classOf[FooSequenceFileFormat].getName))
        }
      })

    implicit val context = newNodeCompilerContext(flowId, jpContext)

    val compiler = NodeCompiler.get(subplan)
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

    implicit val jobContext = newJobContext(sc)

    val input = cls.getConstructor(
      classOf[Map[BroadcastId, Broadcast[_]]],
      classOf[JobContext])
      .newInstance(
        Map.empty,
        jobContext)

    assert(input.branchKeys === Set(inputMarker).map(getBranchKey))

    prepareInput(new File(tmpDir, "foos").getAbsolutePath, configurePath)(0 until 100)

    val rc = newRoundContext()

    val result = Await.result(
      input.compute(rc).apply(getBranchKey(inputMarker)).map {
        _.map {
          case (_, foo: Foo) => foo.id.get
        }.collect.toSeq.sorted
      }, Duration.Inf)

    assert(result === (0 until 100))

    assert(jobContext.inputStatistics(InputCounter.Direct).size === 1)
    val statistics = jobContext.inputStatistics(InputCounter.Direct)(inputOperator.getName)
    assert(statistics.records === 100)
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

    def intToFoo: Int => (NullWritable, Foo) = {

      lazy val foo = new Foo()

      { i =>
        foo.id.modify(i)
        (NullWritable.get, foo)
      }
    }
  }

  class FooSequenceFileFormat extends SequenceFileFormat[NullWritable, Foo, Foo] {

    override def getSupportedType(): Class[Foo] = classOf[Foo]

    override def createKeyObject(): NullWritable = NullWritable.get()

    override def createValueObject(): Foo = new Foo()

    override def copyToModel(key: NullWritable, value: Foo, model: Foo): Unit = {
      model.copyFrom(value)
    }

    override def copyFromModel(model: Foo, key: NullWritable, value: Foo): Unit = {
      value.copyFrom(model)
    }
  }
}
