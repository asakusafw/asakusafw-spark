/*
 * Copyright 2011-2019 Asakusa Framework Team.
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
import org.scalatest.FlatSpec
import org.scalatest.junit.JUnitRunner

import java.io.{ DataInput, DataOutput, File }

import scala.concurrent.{ Await, ExecutionContext, Future }
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.{ NullWritable, Writable }
import org.apache.spark.SparkConf

import com.asakusafw.bridge.stage.StageInfo
import com.asakusafw.lang.compiler.extension.directio.{
  DirectFileIoConstants,
  DirectFileOutputModel
}
import com.asakusafw.lang.compiler.model.description.{ ClassDescription, Descriptions }
import com.asakusafw.lang.compiler.model.graph.{ ExternalOutput, MarkerOperator }
import com.asakusafw.lang.compiler.model.info.ExternalOutputInfo
import com.asakusafw.lang.compiler.planning.PlanMarker
import com.asakusafw.runtime.directio.{ Counter, OutputAttemptContext }
import com.asakusafw.runtime.directio.hadoop.{ HadoopDataSource, HadoopDataSourceUtil, SequenceFileFormat }
import com.asakusafw.runtime.model.DataModel
import com.asakusafw.runtime.value.IntOption
import com.asakusafw.spark.compiler._
import com.asakusafw.spark.compiler.directio.DirectOutputDescription
import com.asakusafw.spark.runtime._
import com.asakusafw.spark.runtime.directio.{ BasicDataDefinition, HadoopObjectFactory }

import com.asakusafw.spark.extensions.iterativebatch.runtime.graph.{
  DirectOutputCommitForIterative,
  IterativeAction
}
import com.asakusafw.spark.extensions.iterativebatch.runtime.{ graph => runtime }

import resource._

@RunWith(classOf[JUnitRunner])
class DirectOutputCommitForIterativeClassBuilderSpecTest extends DirectOutputCommitForIterativeClassBuilderSpec

class DirectOutputCommitForIterativeClassBuilderSpec
  extends FlatSpec
  with SparkForAll
  with FlowIdForEach
  with UsingCompilerContext
  with JobContextSugar
  with RoundContextSugar
  with TempDirForAll {

  import DirectOutputCommitForIterativeClassBuilderSpec._

  behavior of classOf[DirectOutputCommitForIterativeClassBuilder].getSimpleName

  private var root: File = _

  override def configure(conf: SparkConf): SparkConf = {
    val tmpDir = createTempDirectoryForAll("directio-").toFile()
    val system = new File(tmpDir, "system")
    root = new File(tmpDir, "directio")
    conf.setHadoopConf("com.asakusafw.output.system.dir", system.getAbsolutePath)
    conf.setHadoopConf("com.asakusafw.directio.test", classOf[HadoopDataSource].getName)
    conf.setHadoopConf("com.asakusafw.directio.test.path", "test")
    conf.setHadoopConf("com.asakusafw.directio.test.fs.path", root.getAbsolutePath)
  }

  def newCommit(
    outputs: Set[ExternalOutput])(
      prepares: Set[IterativeAction[Unit]])(
        implicit jobContext: JobContext): DirectOutputCommitForIterative = {
    implicit val context = newCompilerContext(flowId)
    val setupType = DirectOutputCommitForIterativeCompiler.compile(outputs)
    context.loadClass(setupType.getClassName)
      .getConstructor(classOf[Set[IterativeAction[Unit]]], classOf[JobContext])
      .newInstance(prepares, jobContext)
  }

  it should "commit" in {
    val foosMarker = MarkerOperator.builder(ClassDescription.of(classOf[Foo]))
      .attribute(classOf[PlanMarker], PlanMarker.GATHER).build()
    val endMarker = MarkerOperator.builder(ClassDescription.of(classOf[Foo]))
      .attribute(classOf[PlanMarker], PlanMarker.END).build()

    val outputOperator = ExternalOutput.builder(
      "out1",
      new ExternalOutputInfo.Basic(
        ClassDescription.of(classOf[DirectFileOutputModel]),
        DirectFileIoConstants.MODULE_NAME,
        ClassDescription.of(classOf[Foo]),
        Descriptions.valueOf(
          new DirectFileOutputModel(
            DirectOutputDescription(
              basePath = "test/out1_${round}",
              resourcePattern = "testing.bin",
              order = Seq.empty,
              deletePatterns = Seq("*.bin"),
              formatType = classOf[FooSequenceFileFormat])))))
      .input(ExternalOutput.PORT_NAME, ClassDescription.of(classOf[Foo]), foosMarker.getOutput)
      .output("end", ClassDescription.of(classOf[Foo]))
      .build()
    outputOperator.findOutput("end").connect(endMarker.getInput)

    val rounds = 0 to 1
    val files = rounds.map { round =>
      new File(root, s"out1_${round}/testing.bin")
    }

    implicit val jobContext = newJobContext(sc)

    val prepare = new Prepare("id", "test/out1_${round}", "testing.bin")("prepare")
    val commit = newCommit(Set(outputOperator))(Set(prepare))

    val origin = newRoundContext()
    val rcs = rounds.map { round =>
      newRoundContext(
        stageId = s"round_${round}",
        batchArguments = Map("round" -> round.toString))
    }

    assert(files.exists(_.exists()) === false)

    Await.result(prepare.perform(origin, rcs), Duration.Inf)
    assert(files.exists(_.exists()) === false)

    Await.result(commit.perform(origin, rcs), Duration.Inf)
    assert(files.forall(_.exists()) === true)
  }

  it should "commit out of scope round" in {
    val foosMarker = MarkerOperator.builder(ClassDescription.of(classOf[Foo]))
      .attribute(classOf[PlanMarker], PlanMarker.GATHER).build()
    val endMarker = MarkerOperator.builder(ClassDescription.of(classOf[Foo]))
      .attribute(classOf[PlanMarker], PlanMarker.END).build()

    val outputOperator = ExternalOutput.builder(
      "out2",
      new ExternalOutputInfo.Basic(
        ClassDescription.of(classOf[DirectFileOutputModel]),
        DirectFileIoConstants.MODULE_NAME,
        ClassDescription.of(classOf[Foo]),
        Descriptions.valueOf(
          new DirectFileOutputModel(
            DirectOutputDescription(
              basePath = "test/out2_${round}",
              resourcePattern = "testing.bin",
              order = Seq.empty,
              deletePatterns = Seq("*.bin"),
              formatType = classOf[FooSequenceFileFormat])))))
      .input(ExternalOutput.PORT_NAME, ClassDescription.of(classOf[Foo]), foosMarker.getOutput)
      .output("end", ClassDescription.of(classOf[Foo]))
      .build()
    outputOperator.findOutput("end").connect(endMarker.getInput)

    val rounds = 0 to 1
    val files = rounds.map { round =>
      new File(root, s"out2_${round}/testing.bin")
    }

    implicit val jobContext = newJobContext(sc)

    val prepare = new Prepare("id", "test/out2_${round}", "testing.bin")("prepare")
    val commit = newCommit(Set(outputOperator))(Set(prepare))

    val origin = newRoundContext()
    val rcs = rounds.map { round =>
      newRoundContext(
        stageId = s"round_${round}",
        batchArguments = Map("round" -> round.toString))
    }

    assert(files.exists(_.exists()) === false)

    Await.result(commit.perform(origin, Seq(rcs.head)), Duration.Inf)
    assert(files.head.exists() === true)
    assert(files.tail.exists(_.exists()) === false)
  }
}

object DirectOutputCommitForIterativeClassBuilderSpec {

  class Prepare(
    id: String,
    basePath: String,
    resourceName: String)(
      val label: String)(
        implicit val jobContext: JobContext)
    extends IterativeAction[Unit] with runtime.CacheAlways[Seq[RoundContext], Future[Unit]] {

    override protected def doPerform(
      origin: RoundContext,
      rcs: Seq[RoundContext])(implicit ec: ExecutionContext): Future[Unit] = Future {
      val conf = origin.hadoopConf.value
      val repository = HadoopDataSourceUtil.loadRepository(conf)

      rcs.foreach { rc =>
        val conf = rc.hadoopConf.value
        val stageInfo = StageInfo.deserialize(conf.get(StageInfo.KEY_NAME))

        val basePath = stageInfo.resolveUserVariables(this.basePath)
        val sourceId = repository.getRelatedId(basePath)
        val containerPath = repository.getContainerPath(basePath)
        val componentPath = repository.getComponentPath(basePath)
        val dataSource = repository.getRelatedDataSource(containerPath)

        val context = new OutputAttemptContext(
          stageInfo.getExecutionId, stageInfo.getStageId, sourceId, new Counter())
        dataSource.setupAttemptOutput(context)

        val definition =
          BasicDataDefinition(new HadoopObjectFactory(conf), classOf[FooSequenceFileFormat])
        for {
          out <- managed(
            dataSource.openOutput(context, definition, componentPath, resourceName, new Counter()))
        } {
          val foo = new Foo()
          foo.id.modify(1)
          out.write(foo)
        }

        dataSource.commitAttemptOutput(context)
        dataSource.cleanupAttemptOutput(context)
      }
    }
  }

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
