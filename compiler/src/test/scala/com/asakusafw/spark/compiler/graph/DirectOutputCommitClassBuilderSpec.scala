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
import org.scalatest.fixture.FlatSpec
import org.scalatest.junit.JUnitRunner

import java.io.{ DataInput, DataOutput, File }

import scala.concurrent.{ Await, ExecutionContext, Future }
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration

import org.apache.hadoop.io.{ NullWritable, Writable }
import org.apache.spark.{ SparkConf, SparkContext }

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
import com.asakusafw.spark.compiler.directio.DirectOutputDescription
import com.asakusafw.spark.compiler.graph.DirectOutputCommitClassBuilderSpec._
import com.asakusafw.spark.runtime._
import com.asakusafw.spark.runtime.directio.{ BasicDataDefinition, HadoopObjectFactory }
import com.asakusafw.spark.runtime.fixture.SparkForAll
import com.asakusafw.spark.runtime.graph._
import com.asakusafw.spark.runtime.{ graph => runtime }

import resource._

@RunWith(classOf[JUnitRunner])
class DirectOutputCommitClassBuilderSpecTest extends DirectOutputCommitClassBuilderSpec

class DirectOutputCommitClassBuilderSpec
  extends FlatSpec
  with SparkForAll
  with FlowIdForEach
  with UsingCompilerContext
  with RoundContextSugar
  with TempDirForAll {

  behavior of classOf[DirectOutputCommitClassBuilder].getSimpleName

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
      prepares: Set[Action[Unit]])(
        implicit sc: SparkContext): DirectOutputCommit = {
    implicit val context = newCompilerContext(flowId)
    val setupType = DirectOutputCommitCompiler.compile(outputs)
    context.loadClass(setupType.getClassName)
      .getConstructor(classOf[Set[Action[Unit]]], classOf[SparkContext])
      .newInstance(prepares, sc)
  }

  it should "commit" in { implicit sc =>
    val foosMarker = MarkerOperator.builder(ClassDescription.of(classOf[Foo]))
      .attribute(classOf[PlanMarker], PlanMarker.GATHER).build()
    val endMarker = MarkerOperator.builder(ClassDescription.of(classOf[Foo]))
      .attribute(classOf[PlanMarker], PlanMarker.END).build()

    val outputOperator = ExternalOutput.builder(
      "out",
      new ExternalOutputInfo.Basic(
        ClassDescription.of(classOf[DirectFileOutputModel]),
        DirectFileIoConstants.MODULE_NAME,
        ClassDescription.of(classOf[Foo]),
        Descriptions.valueOf(
          new DirectFileOutputModel(
            DirectOutputDescription(
              basePath = "test/out",
              resourcePattern = "testing.bin",
              order = Seq.empty,
              deletePatterns = Seq("*.bin"),
              formatType = classOf[FooSequenceFileFormat])))))
      .input(ExternalOutput.PORT_NAME, ClassDescription.of(classOf[Foo]), foosMarker.getOutput)
      .output("end", ClassDescription.of(classOf[Foo]))
      .build()
    outputOperator.findOutput("end").connect(endMarker.getInput)

    val file = new File(root, "out/testing.bin")

    val prepare = new Prepare("id", "test/out/testing.bin", "test/out", "testing.bin")("prepare")
    val commit = newCommit(Set(outputOperator))(Set(prepare))

    val rc = newRoundContext()

    assert(file.exists() === false)

    Await.result(prepare.perform(rc), Duration.Inf)
    assert(file.exists() === false)

    Await.result(commit.perform(rc), Duration.Inf)
    assert(file.exists() === true)
  }
}

object DirectOutputCommitClassBuilderSpec {

  class Prepare(
    id: String,
    physicalPath: String,
    basePath: String,
    resourceName: String)(
      val label: String)(
        implicit val sc: SparkContext)
    extends Action[Unit] with runtime.CacheOnce[RoundContext, Future[Unit]] {

    override protected def doPerform(
      rc: RoundContext)(implicit ec: ExecutionContext): Future[Unit] = Future {
      val conf = rc.hadoopConf.value
      val stageInfo = StageInfo.deserialize(conf.get(StageInfo.KEY_NAME))
      val repository = HadoopDataSourceUtil.loadRepository(conf)

      val sourceId = repository.getRelatedId(basePath)
      val containerPath = repository.getContainerPath(basePath)
      val componentPath = repository.getComponentPath(basePath)
      val dataSource = repository.getRelatedDataSource(containerPath)

      val context = new OutputAttemptContext(
        stageInfo.getExecutionId, "1", sourceId, new Counter())
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
