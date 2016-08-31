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

import scala.concurrent.{ Await, Future }
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration

import org.apache.hadoop.io.{ NullWritable, Writable }
import org.apache.spark.{ SparkConf, SparkContext }

import com.asakusafw.lang.compiler.extension.directio.{
  DirectFileIoConstants,
  DirectFileOutputModel
}
import com.asakusafw.lang.compiler.model.description.{ ClassDescription, Descriptions }
import com.asakusafw.lang.compiler.model.graph.{ ExternalOutput, MarkerOperator }
import com.asakusafw.lang.compiler.model.info.ExternalOutputInfo
import com.asakusafw.lang.compiler.planning.PlanMarker
import com.asakusafw.runtime.directio.hadoop.{ HadoopDataSource, SequenceFileFormat }
import com.asakusafw.runtime.model.DataModel
import com.asakusafw.runtime.value.IntOption
import com.asakusafw.spark.compiler.directio.DirectOutputDescription
import com.asakusafw.spark.compiler.graph.DirectOutputSetupClassBuilderSpec._
import com.asakusafw.spark.runtime._
import com.asakusafw.spark.runtime.fixture.SparkForAll
import com.asakusafw.spark.runtime.graph.DirectOutputSetup

@RunWith(classOf[JUnitRunner])
class DirectOutputSetupClassBuilderSpecTest extends DirectOutputSetupClassBuilderSpec

class DirectOutputSetupClassBuilderSpec
  extends FlatSpec
  with SparkForAll
  with FlowIdForEach
  with UsingCompilerContext
  with RoundContextSugar
  with TempDirForAll {

  behavior of classOf[DirectOutputSetupClassBuilder].getSimpleName

  private var root: File = _

  override def configure(conf: SparkConf): SparkConf = {
    root = createTempDirectoryForAll("directio-").toFile()
    conf.setHadoopConf("com.asakusafw.directio.test", classOf[HadoopDataSource].getName)
    conf.setHadoopConf("com.asakusafw.directio.test.path", "test")
    conf.setHadoopConf("com.asakusafw.directio.test.fs.path", root.getAbsolutePath)
  }

  def newSetup(outputs: Set[ExternalOutput])(implicit sc: SparkContext): DirectOutputSetup = {
    implicit val context = newCompilerContext(flowId)
    val setupType = DirectOutputSetupCompiler.compile(outputs)
    context.loadClass(setupType.getClassName)
      .getConstructor(classOf[SparkContext])
      .newInstance(sc)
  }

  it should "delete simple" in { implicit sc =>
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
              basePath = "test/out1",
              resourcePattern = "*.bin",
              order = Seq.empty,
              deletePatterns = Seq("*.bin"),
              formatType = classOf[FooSequenceFileFormat])))))
      .input(ExternalOutput.PORT_NAME, ClassDescription.of(classOf[Foo]), foosMarker.getOutput)
      .output("end", ClassDescription.of(classOf[Foo]))
      .build()
    outputOperator.findOutput("end").connect(endMarker.getInput)

    val file = new File(root, "out1/testing.bin")
    file.getParentFile.mkdirs()
    file.createNewFile()

    val setup = newSetup(Set(outputOperator))
    val rc = newRoundContext()

    Await.result(setup.perform(rc), Duration.Inf)

    assert(file.exists() === false)
  }

  it should "not delete out of scope" in { implicit sc =>
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
              basePath = "test/out2",
              resourcePattern = "*.bin",
              order = Seq.empty,
              deletePatterns = Seq("*.txt"),
              formatType = classOf[FooSequenceFileFormat])))))
      .input(ExternalOutput.PORT_NAME, ClassDescription.of(classOf[Foo]), foosMarker.getOutput)
      .output("end", ClassDescription.of(classOf[Foo]))
      .build()
    outputOperator.findOutput("end").connect(endMarker.getInput)

    val file = new File(root, "out2/testing.bin")
    file.getParentFile.mkdirs()
    file.createNewFile()

    val setup = newSetup(Set(outputOperator))
    val rc = newRoundContext()

    Await.result(setup.perform(rc), Duration.Inf)

    assert(file.exists() === true)
  }
}

object DirectOutputSetupClassBuilderSpec {

  class Foo extends DataModel[Foo] with Writable {

    val id = new IntOption()
    val group = new IntOption()

    override def reset(): Unit = {
      id.setNull()
      group.setNull()
    }
    override def copyFrom(other: Foo): Unit = {
      id.copyFrom(other.id)
      group.copyFrom(other.group)
    }
    override def readFields(in: DataInput): Unit = {
      id.readFields(in)
      group.readFields(in)
    }
    override def write(out: DataOutput): Unit = {
      id.write(out)
      group.write(out)
    }

    override def toString: String = s"Foo(id=${id}, group=${group})"
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
